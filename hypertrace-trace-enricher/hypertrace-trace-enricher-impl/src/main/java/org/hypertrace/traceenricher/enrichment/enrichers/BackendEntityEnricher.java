package org.hypertrace.traceenricher.enrichment.enrichers;

import com.typesafe.config.Config;
import java.util.List;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.AbstractBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.CassandraBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.ClientSpanEndpointResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.ElasticSearchBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.FqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.GrpcBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.HttpBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.HypertraceFqnResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.JdbcBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.KafkaBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.MongoBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.RabbitMqBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.RedisBackendResolver;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend.SqsBackendResolver;

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
