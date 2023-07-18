package org.hypertrace.traceenricher.enrichment.enrichers.backend;

import com.typesafe.config.Config;
import java.util.List;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.BackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.CassandraBackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.ClientSpanEndpointProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.ElasticSearchBackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.GrpcBackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.HttpBackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.JdbcBackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.KafkaBackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.MongoBackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.RabbitMqBackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.RedisBackendProvider;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.provider.SqsBackendProvider;

public class BackendEntityEnricher extends AbstractBackendEntityEnricher {

  @Override
  public void setup(Config enricherConfig, ClientRegistry clientRegistry) {}

  @Override
  public FqnResolver getFqnResolver() {
    return new HypertraceFqnResolver();
  }

  @Override
  protected Entity mergeBackendEntity(Entity existingEntity, Entity newEntity) {
    Entity.Builder updatedEntityBuilder = Entity.newBuilder(existingEntity);
    updatedEntityBuilder.putAllAttributes(newEntity.getAttributesMap());
    return updatedEntityBuilder.build();
  }

  @Override
  public List<BackendProvider> getBackendProviders() {
    return List.of(
        new HttpBackendProvider(),
        new GrpcBackendProvider(),
        new RedisBackendProvider(),
        new MongoBackendProvider(),
        new JdbcBackendProvider(),
        new CassandraBackendProvider(),
        new ElasticSearchBackendProvider(),
        new RabbitMqBackendProvider(),
        new KafkaBackendProvider(),
        new SqsBackendProvider(),
        new ClientSpanEndpointProvider());
  }
}
