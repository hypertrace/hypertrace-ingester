package org.hypertrace.core.spannormalizer.processor;

public interface SpanNormalizer<Input, Output> {

  Output convert(Input span) throws Exception;
}
