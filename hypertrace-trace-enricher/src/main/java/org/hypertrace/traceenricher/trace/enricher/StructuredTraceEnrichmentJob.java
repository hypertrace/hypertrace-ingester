package org.hypertrace.traceenricher.trace.enricher;

import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.ENRICHER_CONFIG_TEMPLATE;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.ENRICHER_NAMES_CONFIG;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.FLINK_JOB;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.FLINK_SINK_CONFIG_PATH;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.FLINK_SOURCE_CONFIG_PATH;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.KAFKA_CONFIG_PATH;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.LOG_FAILURES_CONFIG;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.METRICS;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.PARALLELISM;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.SCHEMA_REGISTRY_CONFIG_PATH;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnrichmentJob.JobConfig.TOPIC_NAME_CONFIG;

import com.typesafe.config.Config;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import java.util.LinkedHashMap;
import java.util.List;
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
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.flinkutils.avro.RegistryBasedAvroSerde;
import org.hypertrace.core.flinkutils.utils.FlinkUtils;
import org.hypertrace.core.serviceframework.background.PlatformBackgroundJob;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.traceenricher.enrichment.EnrichmentRegistry;

public class StructuredTraceEnrichmentJob implements PlatformBackgroundJob {

  private final String inputTopic;
  private final String outputTopic;
  private final Properties kafkaConsumerConfig;
  private final Properties kafkaProducerConfig;
  private final Map<String, String> sourceSchemaRegistryConfig;
  private final Map<String, String> sinkSchemaRegistryConfig;
  private final boolean logFailuresOnly;
  private final ProcessFunction processFunction;

  private final Map<String, String> metricsConfig;
  private StreamExecutionEnvironment environment;

  public StructuredTraceEnrichmentJob(Config configs) {
    Config jobConfig = configs.getConfig(FLINK_JOB);
    metricsConfig = ConfigUtils.getFlatMapConfig(jobConfig, METRICS);

    Config flinkSourceConfig = configs.getConfig(FLINK_SOURCE_CONFIG_PATH);
    this.inputTopic = flinkSourceConfig.getString(TOPIC_NAME_CONFIG);
    this.kafkaConsumerConfig = ConfigUtils.getPropertiesConfig(flinkSourceConfig, KAFKA_CONFIG_PATH);
    this.sourceSchemaRegistryConfig = ConfigUtils.getFlatMapConfig(flinkSourceConfig, SCHEMA_REGISTRY_CONFIG_PATH);

    Config flinkSinkConfig = configs.getConfig(FLINK_SINK_CONFIG_PATH);
    this.outputTopic = flinkSinkConfig.getString(TOPIC_NAME_CONFIG);
    this.logFailuresOnly = ConfigUtils.getBooleanConfig(flinkSinkConfig, LOG_FAILURES_CONFIG, true);
    this.kafkaProducerConfig = ConfigUtils.getPropertiesConfig(flinkSinkConfig, KAFKA_CONFIG_PATH);
    this.sinkSchemaRegistryConfig = ConfigUtils.getFlatMapConfig(flinkSinkConfig, SCHEMA_REGISTRY_CONFIG_PATH);

    List<String> enrichers = configs.getStringList(ENRICHER_NAMES_CONFIG);

    Map<String, Config> enricherConfigs = new LinkedHashMap<>();
    for (String enricher : enrichers) {
      Config enricherConfig = configs.getConfig(getEnricherConfigPath(enricher));
      enricherConfigs.put(enricher, enricherConfig);
    }
    this.processFunction = getEnrichmentProcessFunction(enricherConfigs);
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

    DeserializationSchema<StructuredTrace> deserializationSchema = new RegistryBasedAvroSerde<>(inputTopic,
        StructuredTrace.class, sourceSchemaRegistryConfig
    );
    FlinkKafkaConsumer<?> structuredTraceKafkaConsumer =
        FlinkUtils.createKafkaConsumer(inputTopic, deserializationSchema, kafkaConsumerConfig);

    DataStream<?> inputStream = environment.addSource(structuredTraceKafkaConsumer);

    final SingleOutputStreamOperator<SerializationSchema> outputStream = inputStream.process(processFunction);

    SerializationSchema<StructuredTrace> serializationSchema = new RegistryBasedAvroSerde<>(outputTopic,
        StructuredTrace.class, sinkSchemaRegistryConfig
    );

    outputStream.addSink(
        FlinkUtils.getFlinkKafkaProducer(
            outputTopic,
            serializationSchema,
            kafkaProducerConfig,
            logFailuresOnly
        )
    );
    environment.execute("trace-enricher");
  }

  @Override
  public void stop() {

  }

  private static String getEnricherConfigPath(String enricher) {
    return String.format(ENRICHER_CONFIG_TEMPLATE, enricher);
  }

  private static ProcessFunction getEnrichmentProcessFunction(Map<String, Config> enricherConfigs) {
    EnrichmentRegistry registry = new EnrichmentRegistry();
    registry.registerEnrichers(enricherConfigs);
    return new StructuredTraceEnrichProcessor(registry);
  }

  static class JobConfig {
    public static final String FLINK_SOURCE_CONFIG_PATH = "flink.source";
    public static final String FLINK_SINK_CONFIG_PATH = "flink.sink";
    public static final String TOPIC_NAME_CONFIG = "topic";
    public static final String KAFKA_CONFIG_PATH = "kafka";
    public static final String SCHEMA_REGISTRY_CONFIG_PATH = "schema.registry";
    public static final String LOG_FAILURES_CONFIG = "log.failures.only";

    public static final String ENRICHER_NAMES_CONFIG = "enricher.names";
    public static final String ENRICHER_CONFIG_TEMPLATE = "enricher.%s";
    public static final String FLINK_JOB = "flink.job";
    public static final String METRICS = "metrics";
    public static final String PARALLELISM = "parallelism";
  }
}
