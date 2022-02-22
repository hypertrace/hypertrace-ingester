package org.hypertrace.core.spannormalizer.jaeger;

public class SpanDropFilter {

  public static final String TAG_KEY = "tagKey";
  public static final String OPERATOR = "operator";
  public static final String TAG_VALUE = "tagValue";

  public enum Operator {
    EQ("EQ"),
    NEQ("NEQ"),
    EXISTS("EXISTS"), // need to get rid of this
    CONTAINS("CONTAINS"),
    STARTS_WITH("STARTS_WITH"),
    ENDS_WITH("ENDS_WITH"),
    REGEX_MATCH("REGEX_MATCH");

    private final String value;

    Operator(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private String tagKey;
  private Operator operator;
  private String tagValue;

  public SpanDropFilter(String tagKey, String operator, String tagValue) {
    this.tagKey = tagKey;
    this.operator = Operator.valueOf(operator);
    this.tagValue = tagValue;
  }

  public String getTagKey() {
    return tagKey;
  }

  public Operator getOperator() {
    return operator;
  }

  public String getTagValue() {
    return tagValue;
  }

  @Override
  public String toString() {
    return "SpanDropFilter{"
        + "tagKey='"
        + tagKey
        + '\''
        + ", operator="
        + operator
        + ", tagValue='"
        + tagValue
        + '\''
        + '}';
  }
}
