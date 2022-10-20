package org.hypertrace.core.spannormalizer.redaction;

import static org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants.REDACTED_FIELD_PREFIX;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;

public class PIIPCIField {

  public static class RegexInfo {
    private String regexString;
    private Pattern regexPattern;

    public RegexInfo(String regexString) {
      this.regexString = regexString;
      this.regexPattern = Pattern.compile(regexString);
    }

    public String getRegexString() {
      return regexString;
    }

    public Pattern getRegexPattern() {
      return regexPattern;
    }

    @Override
    public String toString() {
      return "RegexInfo{"
          + "regexString='"
          + regexString
          + '\''
          + ", regexPattern="
          + regexPattern
          + '}';
    }
  }

  public enum PIIPCIFieldType {
    PII("PII"),
    PCI("PCI");

    private final String value;

    PIIPCIFieldType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private String name;
  private Set<String> tagKeySet;
  private RegexInfo regexInfo;
  private AttributeValue replacementValue;
  private PIIPCIFieldType piiPciFieldType;

  public PIIPCIField(String name, String regex, List<String> tagKeyList, String type) {
    if (isValidConfig(regex, tagKeyList, type)) {
      this.name = name;
      this.tagKeySet = new HashSet<>(tagKeyList);
      if (!StringUtils.isEmpty(regex)) {
        this.regexInfo = new RegexInfo(regex);
      }
      this.replacementValue =
          AttributeValue.newBuilder().setValue(REDACTED_FIELD_PREFIX + name).build();
      this.piiPciFieldType = PIIPCIFieldType.valueOf(type);
    }
  }

  private boolean isValidConfig(String regex, List<String> tagKeyList, String type) {
    if (StringUtils.isEmpty(regex) && tagKeyList.isEmpty()) {
      throw new RuntimeException("Both Regex and TagKeyList cannot be empty.");
    }
    if ((PIIPCIFieldType.valueOf(type) != PIIPCIFieldType.PCI)
        && (PIIPCIFieldType.valueOf(type) != PIIPCIFieldType.PII)) {
      throw new RuntimeException("Invalid PII/PCI field type provided.");
    }
    return true;
  }

  public Set<String> getTagKeySet() {
    return tagKeySet;
  }

  public String getName() {
    return name;
  }

  public AttributeValue getReplacementValue() {
    return replacementValue;
  }

  public Optional<RegexInfo> getRegexInfo() {
    return Optional.ofNullable(regexInfo);
  }

  public PIIPCIFieldType getPiiPciFieldType() {
    return piiPciFieldType;
  }
}
