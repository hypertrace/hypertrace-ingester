package org.hypertrace.traceenricher.enrichedspan.constants;

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
