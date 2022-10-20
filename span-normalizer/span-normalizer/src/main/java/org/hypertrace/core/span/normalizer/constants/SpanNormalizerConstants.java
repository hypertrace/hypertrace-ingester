package org.hypertrace.core.spannormalizer.constants;

public class SpanNormalizerConstants {
  public static final String INPUT_TOPIC_CONFIG_KEY = "input.topic";
  public static final String OUTPUT_TOPIC_CONFIG_KEY = "output.topic";
  public static final String OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY = "raw.logs.output.topic";
  public static final String SPAN_NORMALIZER_JOB_CONFIG = "span-normalizer-job-config";
  public static final String BYPASS_OUTPUT_TOPIC_CONFIG_KEY = "bypass.output.topic";
  public static final String SPAN_REDACTION_CONFIG_KEY = "spanRedaction";
  public static final String PII_PCI_CONFIG_KEY = "spanRedaction.piiPciFields";
  public static final String REDACTED_FIELD_PREFIX = "redacted-";
  public static final String REDACTED_PII_TAGS_KEY = "redacted.pii.count";
  public static final String REDACTED_PCI_TAGS_KEY = "redacted.pci.count";
}
