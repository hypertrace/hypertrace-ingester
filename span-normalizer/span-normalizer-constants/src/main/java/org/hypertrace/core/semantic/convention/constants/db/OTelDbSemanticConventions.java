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
  OTHER_SQL_DB_SYSTEM_VALUE("other_sql"),
  DB2_DB_SYSTEM_VALUE("db2"),
  POSTGRESQL_DB_SYSTEM_VALUE("postgresql"),
  REDSHIFT_DB_SYSTEM_VALUE("redshift"),
  HIVE_DB_SYSTEM_VALUE("hive"),
  CLOUDSCAPE_DB_SYSTEM_VALUE("cloudspace"),
  HSQLDB_DB_SYSTEM_VALUE("hsqldb"),
  MONGODB_COLLECTION("db.mongodb.collection");

  private final String value;

  OTelDbSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
