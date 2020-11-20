package org.hypertrace.attribute.db;

public enum OTelDbAttributes {
  DB_SYSTEM("db.system"),
  DB_CONNECTION_STRING("db.connection_string"),
  DB_OPERATION("db.operation"),
  DB_STATEMENT("db.statement"),
  NET_PEER_IP("net.peer.ip"),
  NET_PEER_PORT("net.peer.port"),
  NET_PEER_NAME("net.peer.name"),
  NET_TRANSPORT("net.transport"),
  MONGODB_DB_SYSTEM_VALUE("mongodb"),
  REDIS_DB_SYSTEM_VALUE("redis"),
  MONGODB_COLLECTION("db.mongodb.collection");

  private final String value;

  OTelDbAttributes(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
