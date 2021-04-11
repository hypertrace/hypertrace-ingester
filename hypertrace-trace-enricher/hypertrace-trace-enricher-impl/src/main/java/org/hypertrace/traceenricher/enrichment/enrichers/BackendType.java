package org.hypertrace.traceenricher.enrichment.enrichers;

public enum BackendType {
  UNKNOWN,
  HTTP,
  HTTPS,
  GRPC,
  REDIS,
  MONGO,
  JDBC,
  CASSANDRA,
  ELASTICSEARCH,
  RABBIT_MQ,
  KAFKA,
  SQS
}
