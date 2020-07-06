package org.hypertrace.core.spannormalizer;

import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.FLINK_JOB;
import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.FLINK_SINK_CONFIG_PATH;
import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.FLINK_SOURCE_CONFIG_PATH;
import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.KAFKA_CONFIG_PATH;
import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.LOG_FAILURES_CONFIG;
import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.METRICS;
import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.PARALLELISM;
import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.SCHEMA_REGISTRY_CONFIG_PATH;
import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.SPAN_TYPE_CONFIG;
import static org.hypertrace.core.spannormalizer.SpanNormalizerJob.JobConfig.TOPIC_NAME_CONFIG;

import com.typesafe.config.Config;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.flinkutils.avro.AvroPayloadSchema;
import org.hypertrace.core.flinkutils.avro.RegistryBasedAvroSerde;
import org.hypertrace.core.flinkutils.utils.FlinkUtils;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundJob;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanSchema;
import org.hypertrace.core.spannormalizer.jaeger.JaegerSpanToAvroRawSpanProcessor;

public class SpanNormalizerJob implements PlatformBackgroundJob {
  private final String spanType;

  private final String inputTopic;
  private final String outputTopic;
  private final Properties kafkaConsumerConfig;
  private final Properties kafkaProducerConfig;
  private final Map<String, String> sinkSchemaRegistryConfig;
  private final boolean logFailuresOnly;
  private final ProcessFunction processFunction;
  private final StreamExecutionEnvironment environment;

  SpanNormalizerJob(Config config) {
    this.spanType = config.getString(SPAN_TYPE_CONFIG);

    Config jobConfig = config.getConfig(FLINK_JOB);
    Map<String, String> metricsConfig = ConfigUtils.getFlatMapConfig(jobConfig, METRICS);

    Config flinkSourceConfig = config.getConfig(FLINK_SOURCE_CONFIG_PATH);
    this.inputTopic = flinkSourceConfig.getString(TOPIC_NAME_CONFIG);
    this.kafkaConsumerConfig =
        ConfigUtils.getPropertiesConfig(flinkSourceConfig, KAFKA_CONFIG_PATH);

    Config flinkSinkConfig = config.getConfig(FLINK_SINK_CONFIG_PATH);
    this.outputTopic = flinkSinkConfig.getString(TOPIC_NAME_CONFIG);
    this.logFailuresOnly = ConfigUtils.getBooleanConfig(flinkSinkConfig, LOG_FAILURES_CONFIG, true);
    this.sinkSchemaRegistryConfig =
        ConfigUtils.getFlatMapConfig(flinkSinkConfig, SCHEMA_REGISTRY_CONFIG_PATH);
    this.kafkaProducerConfig = ConfigUtils.getPropertiesConfig(flinkSinkConfig, KAFKA_CONFIG_PATH);

    this.processFunction = getProtoSpanToRawSpanAggregateFunction(this.spanType, config);

    environment = FlinkUtils.getExecutionEnvironment(metricsConfig);
    int parallelism = jobConfig.getInt(PARALLELISM);
    environment.setParallelism(parallelism);
  }

  public void run() throws Exception {
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    environment
        .getConfig()
        .addDefaultKryoSerializer(
            Class.forName("java.util.Collections$UnmodifiableCollection"),
            UnmodifiableCollectionsSerializer.class);
    environment.getConfig().enableForceKryo();
    FlinkKafkaConsumer<?> spanKafkaConsumer =
        FlinkUtils.createKafkaConsumer(
            inputTopic, getSpanDeserializationSchema(this.spanType), kafkaConsumerConfig);

    DataStream<?> inputStream = environment.addSource(spanKafkaConsumer);

    final SingleOutputStreamOperator outputStream = inputStream.process(processFunction);

    SerializationSchema<RawSpan> serializationSchema =
        new RegistryBasedAvroSerde<>(outputTopic, RawSpan.class, sinkSchemaRegistryConfig);
    outputStream.addSink(
        FlinkUtils.getFlinkKafkaProducer(
            outputTopic, serializationSchema, kafkaProducerConfig, logFailuresOnly));
    environment.execute(SpanNormalizerJob.class.getSimpleName());
  }

  public void stop() {}

  private ProcessFunction getProtoSpanToRawSpanAggregateFunction(String spanType, Config config) {
    switch (spanType) {
      case "jaeger":
      default:
        return new JaegerSpanToAvroRawSpanProcessor(config);
    }
  }

  private DeserializationSchema getSpanDeserializationSchema(String spanType) {
    switch (spanType) {
      case "jaeger":
        return new JaegerSpanSchema();
      case "rawSpan":
        return new AvroPayloadSchema(RawSpan.class.getCanonicalName());
      default:
        throw new UnsupportedOperationException("Cannot recognize span type: " + spanType);
    }
  }

  static class JobConfig {
    public static String SPAN_TYPE_CONFIG = "span.type";

    public static final String FLINK_SOURCE_CONFIG_PATH = "flink.source";
    public static final String FLINK_SINK_CONFIG_PATH = "flink.sink";
    public static final String TOPIC_NAME_CONFIG = "topic";
    public static final String SCHEMA_REGISTRY_CONFIG_PATH = "schema.registry";
    public static final String KAFKA_CONFIG_PATH = "kafka";
    public static final String LOG_FAILURES_CONFIG = "log.failures.only";
    public static final String FLINK_JOB = "flink.job";
    public static final String METRICS = "metrics";
    public static final String PARALLELISM = "parallelism";
  }
}
