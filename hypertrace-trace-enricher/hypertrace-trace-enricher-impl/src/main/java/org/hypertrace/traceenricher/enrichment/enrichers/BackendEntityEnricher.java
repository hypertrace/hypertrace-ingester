package org.hypertrace.traceenricher.enrichment.enrichers;

import com.typesafe.config.Config;
import java.util.List;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.AbstractBackendEntityEnricher;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.AbstractBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.CassandraBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.ClientSpanEndpointResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.ElasticSearchBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.FqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.GrpcBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.HttpBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.HypertraceFqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.JdbcBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.KafkaBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.MongoBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.RabbitMqBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.RedisBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.backend.resolver.SqsBackendResolver;

public class BackendEntityEnricher extends AbstractBackendEntityEnricher {
  private FqnResolver fqnResolver;

  @Override
  public void setup(Config enricherConfig, ClientRegistry clientRegistry) {
    this.fqnResolver = new HypertraceFqnResolver();
  }

  @Override
  public List<AbstractBackendResolver> getBackendResolvers() {
    return List.of(
        new HttpBackendResolver(fqnResolver),
        new GrpcBackendResolver(fqnResolver),
        new RedisBackendResolver(fqnResolver),
        new MongoBackendResolver(fqnResolver),
        new JdbcBackendResolver(fqnResolver),
        new CassandraBackendResolver(fqnResolver),
        new ElasticSearchBackendResolver(fqnResolver),
        new RabbitMqBackendResolver(fqnResolver),
        new KafkaBackendResolver(fqnResolver),
        new SqsBackendResolver(fqnResolver),
        new ClientSpanEndpointResolver(fqnResolver));
  }
}
