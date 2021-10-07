package org.hypertrace.metrics.generator;

import static java.util.stream.Collectors.joining;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class Metric {
  private static final String DELIMITER = ":";

  private String name;
  private Map<String, String> attributes;
  private String key;

  public Metric(String name, Map<String, String> attributes) {
    this.name = name;
    this.attributes = attributes;
    this.key = generateKey();
  }

  private String generateKey() {
    String attributesStr = attributes.entrySet()
        .stream()
        .map(Object::toString)
        .collect(joining(DELIMITER));

    String id =  String.join(DELIMITER,
        name, attributesStr);

    return UUID.nameUUIDFromBytes(id.getBytes()).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof Metric))
      return false;
    Metric other = (Metric) o;

    return this.name.equals(other.name) && this.key.equals(other.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key);
  }
}
