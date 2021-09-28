package org.hypertrace.traceenricher.processor;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.client.EdsCacheClient;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.traceenricher.enrichment.EnricherConfigFactory;
import org.hypertrace.traceenricher.enrichment.EnrichmentProcessor;
import org.hypertrace.traceenricher.enrichment.EnrichmentRegistry;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StructuredTracesEnrichmentTest {
  private static final String ENRICHER_CONFIG_FILE_NAME = "enricher.conf";
  private static final String TENANT_ID = "__default";

  private EnrichmentProcessor enrichmentProcessor;
  @Mock private EdsCacheClient edsClient;
  @Mock private ClientRegistry clientRegistry;
  private EntityCache entityCache;

  @BeforeEach
  public void setup() {
    // Clear any stale entries in the entities cache.
    String configFilePath =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource(ENRICHER_CONFIG_FILE_NAME)
            .getPath();
    if (configFilePath == null) {
      throw new RuntimeException(
          "Cannot find enricher config file" + ENRICHER_CONFIG_FILE_NAME + "in the classpath");
    }

    Config fileConfig = ConfigFactory.parseFile(new File(configFilePath));
    Config configs = ConfigFactory.load(fileConfig);
    // Not passing the Entity Data Service configuration, unless the container id
    // in the span data in inside EDS
    when(clientRegistry.getEdsCacheClient()).thenReturn(edsClient);
    entityCache = new EntityCache(edsClient);
    when(clientRegistry.getEntityCache()).thenReturn(entityCache);

    enrichmentProcessor = createEnricherProcessor(configs);
    mockGetServiceEntityMethod();
  }

  private void mockGetServiceEntityMethod() {
    String[] serviceNames = new String[] {"api_01", "api_02", "api_03"};
    for (String serviceName : serviceNames) {
      org.hypertrace.entity.data.service.v1.AttributeValue value =
          org.hypertrace.entity.data.service.v1.AttributeValue.newBuilder()
              .setValue(Value.newBuilder().setString(serviceName))
              .build();
      org.hypertrace.entity.data.service.v1.Entity serviceEntity =
          org.hypertrace.entity.data.service.v1.Entity.newBuilder()
              .setTenantId(TENANT_ID)
              .setEntityName(serviceName)
              .setEntityId(serviceName + "-id")
              .setEntityType(EntityType.SERVICE.name())
              .putIdentifyingAttributes(
                  EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN), value)
              .build();
      when(edsClient.getEntitiesByName(TENANT_ID, EntityType.SERVICE.name(), serviceName))
          .thenReturn(Collections.singletonList(serviceEntity));

      ByTypeAndIdentifyingAttributes request =
          ByTypeAndIdentifyingAttributes.newBuilder()
              .setEntityType(EntityType.SERVICE.name())
              .putIdentifyingAttributes(
                  EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN), value)
              .build();

      doReturn(serviceEntity).when(edsClient).upsert(serviceEntity);
      doReturn(serviceEntity).when(edsClient).getByTypeAndIdentifyingAttributes(TENANT_ID, request);
    }
  }

  private EnrichmentProcessor createEnricherProcessor(Config configs) {
    Map<String, Config> enricherToRegister = EnricherConfigFactory.createEnricherConfig(configs);
    EnrichmentRegistry enrichmentRegistry = new EnrichmentRegistry();
    enrichmentRegistry.registerEnrichers(enricherToRegister);
    return new EnrichmentProcessor(
        enrichmentRegistry.getOrderedRegisteredEnrichers(),
        clientRegistry,
        configs.getConfig("enricher.executors"));
  }

  @Test
  public void testTraceMissingDownstreamEntrySpans() throws IOException, URISyntaxException {
    String schemaStr =
        readStructuredTraceSchema("missing-downstream-entry-spans/structured-trace-schema.json");
    Schema schema = (new Schema.Parser()).parse(schemaStr);

    StructuredTrace structuredTrace =
        readInStructuredTraceFromJson(
            "missing-downstream-entry-spans/before-enrichment.json", schema);
    StructuredTrace expectedEnrichedStructuredTrace =
        readInStructuredTraceFromJson(
            "missing-downstream-entry-spans/after-enrichment.json", schema);
    enrichmentProcessor.process(structuredTrace);
    Assertions.assertEquals(6, structuredTrace.getEventList().size());
    Assertions.assertEquals(expectedEnrichedStructuredTrace, structuredTrace);
  }

  private static StructuredTrace readInStructuredTraceFromJson(String traceFileName, Schema schema)
      throws IOException {
    URL resource = Thread.currentThread().getContextClassLoader().getResource(traceFileName);
    if (resource == null) {
      throw new RuntimeException("Cannot find " + traceFileName + " in the classpath");
    }

    InputStream din = resource.openStream();
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
    DatumReader<StructuredTrace> reader =
        new SpecificDatumReader<>(schema, StructuredTrace.getClassSchema());

    StructuredTrace structuredTrace = null;

    while (true) {
      try {
        structuredTrace = reader.read(null, decoder);
      } catch (EOFException eofException) {
        break;
      }
    }

    return structuredTrace;
  }

  private String readStructuredTraceSchema(String schemaResourceName)
      throws IOException, URISyntaxException {
    StringBuilder sb = new StringBuilder();
    URL resource = Thread.currentThread().getContextClassLoader().getResource(schemaResourceName);
    Stream<String> stream = Files.lines(Paths.get(resource.toURI()), StandardCharsets.UTF_8);
    stream.forEach(sb::append);
    return sb.toString();
  }
}
