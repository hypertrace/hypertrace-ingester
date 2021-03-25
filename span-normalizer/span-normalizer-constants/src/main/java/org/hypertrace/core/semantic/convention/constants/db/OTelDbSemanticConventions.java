package org.hypertrace.core.semantic.convention.constants.db;

/** OTEL specific attributes for database */
public enum OTelDbSemanticConventions {
  DB_SYSTEM("db.system"),
  DB_CONNECTION_STRING("db.connection_string"),
  DB_OPERATION("db.operation"),
  DB_STATEMENT("db.statement"),
  MONGODB_DB_SYSTEM_VALUE("mongodb"),
  REDIS_DB_SYSTEM_VALUE("redis"),
  MYSQL_DB_SYSTEM_VALUE("mysql"),
  ORACLE_DB_SYSTEM_VALUE("oracle"),
  MSSQL_DB_SYSTEM_VALUE("mssql"),
  CASSANDRA_DB_SYSTEM_VALUE("cassandra"),
  ELASTICSEARCH_DB_SYSTEM_VALUE("elasticsearch"),
  OTHER_SQL_DB_SYSTEM_VALUE("other_sql"),
  DB2_DB_SYSTEM_VALUE("db2"),
  POSTGRESQL_DB_SYSTEM_VALUE("postgresql"),
  REDSHIFT_DB_SYSTEM_VALUE("redshift"),
  HIVE_DB_SYSTEM_VALUE("hive"),
  CLOUDSCAPE_DB_SYSTEM_VALUE("cloudspace"),
  HSQLDB_DB_SYSTEM_VALUE("hsqldb"),
  MONGODB_COLLECTION("db.mongodb.collection"),
  DB_NAME("db.name"),
  REDIS_DB_INDEX("db.redis.database_index"),
  SQL_TABLE_NAME("db.sql.table"),
  CASSANDRA_DB_NAME("db.cassandra.keyspace"),
  CASSANDRA_TABLE_NAME("db.cassandra.table"),
  CASSANDRA_PAGE_SIZE("db.cassandra.page_size"),
  ELASTICSEARCH_ACTION("elasticsearch.action"),
  ELASTICSEARCH_REQUEST("elasticsearch.request"),
  ELASTICSEARCH_REQUEST_INDICES("elasticsearch.request.indices"),
  ELASTICSEARCH_URL("elasticsearch.url"),
  ELASTICSEARCH_METHOD("elasticsearch.method");

  private final String value;

  OTelDbSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
