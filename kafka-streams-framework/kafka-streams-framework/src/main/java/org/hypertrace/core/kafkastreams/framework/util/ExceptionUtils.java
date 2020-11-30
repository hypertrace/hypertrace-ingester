package org.hypertrace.core.kafkastreams.framework.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtils {

  public static String getStackTrace(final Throwable throwable) {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw, true);
    throwable.printStackTrace(pw);
    return sw.getBuffer().toString();
  }
}
