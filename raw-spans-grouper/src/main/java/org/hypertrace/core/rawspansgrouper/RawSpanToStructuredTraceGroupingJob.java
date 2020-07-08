package org.hypertrace.core.rawspansgrouper;

import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.FLINK_JOB;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.FLINK_SINK_CONFIG_PATH;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.FLINK_SOURCE_CONFIG_PATH;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.KAFKA_CONFIG_PATH;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.LOG_FAILURES_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.METRICS;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.PARALLELISM;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.SCHEMA_REGISTRY_CONFIG_PATH;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.SPAN_TYPE_CONFIG;
import static org.hypertrace.core.rawspansgrouper.RawSpanToStructuredTraceGroupingJob.JobConfig.TOPIC_NAME_CONFIG;

import com.typesafe.config.Config;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.flinkutils.avro.RegistryBasedAvroSerde;
import org.hypertrace.core.flinkutils.utils.FlinkUtils;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundJob;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawSpanToStructuredTraceGroupingJob implements PlatformBackgroundJob {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(RawSpanToStructuredTraceGroupingJob.class);

  private final String spanType;
  private final String inputTopic;
  private final String outputTopic;
  private final Properties kafkaConsumerConfig;
  private final Properties kafkaProducerConfig;
  private final Map<String, String> sourceSchemaRegistryConfig;
  private final Map<String, String> sinkSchemaRegistryConfig;
  private final boolean logFailuresOnly;

  private AggregateFunction aggregateFunction;
  private int groupbySessionWindowInterval;

  private final Map<String, String> metricsConfig;
  private StreamExecutionEnvironment environment;

  RawSpanToStructuredTraceGroupingJob(Config configs) {
    this.spanType = configs.getString(SPAN_TYPE_CONFIG);

    Config jobConfig = configs.getConfig(FLINK_JOB);
    metricsConfig = ConfigUtils.getFlatMapConfig(jobConfig, METRICS);

    Config flinkSourceConfig = configs.getConfig(FLINK_SOURCE_CONFIG_PATH);
    this.inputTopic = flinkSourceConfig.getString(TOPIC_NAME_CONFIG);
    this.kafkaConsumerConfig = ConfigUtils
        .getPropertiesConfig(flinkSourceConfig, KAFKA_CONFIG_PATH);
    this.sourceSchemaRegistryConfig = ConfigUtils
        .getFlatMapConfig(flinkSourceConfig, SCHEMA_REGISTRY_CONFIG_PATH);

    Config flinkSinkConfig = configs.getConfig(FLINK_SINK_CONFIG_PATH);
    this.outputTopic = flinkSinkConfig.getString(TOPIC_NAME_CONFIG);
    this.logFailuresOnly = flinkSinkConfig.getBoolean(LOG_FAILURES_CONFIG);
    this.kafkaProducerConfig = ConfigUtils.getPropertiesConfig(flinkSinkConfig, KAFKA_CONFIG_PATH);
    this.sinkSchemaRegistryConfig = ConfigUtils
        .getFlatMapConfig(flinkSinkConfig, SCHEMA_REGISTRY_CONFIG_PATH);

    this.groupbySessionWindowInterval = configs.getInt(SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG);
    this.aggregateFunction = getTraceGroupAggregateFunction(spanType);
    environment = FlinkUtils.getExecutionEnvironment(metricsConfig);

    int parallelism = jobConfig.getInt(PARALLELISM);
    environment.setParallelism(parallelism);
  }

  @Override
  public void run() throws Exception {
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    environment.getConfig()
        .addDefaultKryoSerializer(Class.forName("java.util.Collections$UnmodifiableCollection"),
            UnmodifiableCollectionsSerializer.class);
    environment.getConfig().enableForceKryo();

    DeserializationSchema<RawSpan> deserializationSchema = new RegistryBasedAvroSerde<>(inputTopic,
        RawSpan.class, sourceSchemaRegistryConfig
    );
    FlinkKafkaConsumer<?> spanKafkaConsumer =
        FlinkUtils.createKafkaConsumer(inputTopic, deserializationSchema, kafkaConsumerConfig);

    DataStream<?> inputStream = environment.addSource(spanKafkaConsumer);

    final SingleOutputStreamOperator<SerializationSchema> outputStream = inputStream
        .keyBy(getSpanKeySelector(this.spanType))
        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(groupbySessionWindowInterval)))
        .aggregate(aggregateFunction);
    SerializationSchema<StructuredTrace> serializationSchema = new RegistryBasedAvroSerde<>(
        outputTopic, StructuredTrace.class, sinkSchemaRegistryConfig);
    outputStream.addSink(
        FlinkUtils
            .getFlinkKafkaProducer(outputTopic, serializationSchema,
                null/* By specifying null this will default to kafka producer's default partitioner i.e. round-robin*/,
                kafkaProducerConfig,
                logFailuresOnly));
    environment.execute("raw-spans-grouper");
  }

  @Override
  public void stop() {
  }

  private AggregateFunction getTraceGroupAggregateFunction(String spanType) {
    switch (spanType) {
      case "rawSpan":
        return new RawSpanToStructuredTraceAvroGroupAggregator();
      default:
        throw new UnsupportedOperationException("Cannot recognize span type: " + spanType);
    }
  }

  private KeySelector getSpanKeySelector(String spanType) {
    switch (spanType) {
      case "rawSpan":
        return getRawSpanAvroKeySelector();
      default:
        throw new UnsupportedOperationException("Cannot recognize span type: " + spanType);
    }
  }

  /**
   * A note about this method. Due to how lax we are with generic typing for Flink stream objects
   * initialization, this methods needs to be static. Otherwise, it throws NPE on a class
   * initialization. We should clean up all the wildcard generic types that we use in the background
   * job classes.
   */
  private static KeySelector getRawSpanAvroKeySelector() {
    return new KeySelector<RawSpan, Tuple2<String, String>>() {
      @Override
      public Tuple2<String, String> getKey(RawSpan value) {
        return new Tuple2<String, String>(value.getCustomerId(),
            new String(value.getTraceId().array()));
      }
    };
  }

  static class JobConfig {

    public static String SPAN_TYPE_CONFIG = "span.type";

    public static final String FLINK_SOURCE_CONFIG_PATH = "flink.source";
    public static final String FLINK_SINK_CONFIG_PATH = "flink.sink";
    public static final String TOPIC_NAME_CONFIG = "topic";
    public static final String KAFKA_CONFIG_PATH = "kafka";
    public static final String SCHEMA_REGISTRY_CONFIG_PATH = "schema.registry";
    public static final String LOG_FAILURES_CONFIG = "log.failures.only";

    public static final String SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG = "span.groupby.session.window.interval";
    public static final String FLINK_JOB = "flink.job";
    public static final String METRICS = "metrics";
    public static final String PARALLELISM = "parallelism";
  }
}
