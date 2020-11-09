package org.hypertrace.trace.reader;

import org.hypertrace.core.attribute.service.v1.LiteralValue;

public class LiteralValueUtil {

  public static LiteralValue stringLiteral(String stringValue) {
    return LiteralValue.newBuilder().setStringValue(stringValue).build();
  }

  public static LiteralValue longLiteral(Number number) {
    return LiteralValue.newBuilder().setIntValue(number.longValue()).build();
  }

  public static LiteralValue doubleLiteral(Number number) {
    return LiteralValue.newBuilder().setFloatValue(number.doubleValue()).build();
  }

  public static LiteralValue booleanLiteral(boolean booleanValue) {
    return LiteralValue.newBuilder().setBooleanValue(booleanValue).build();
  }
}
