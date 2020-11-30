package org.hypertrace.core.kafkastreams.framework.exceptionhandlers;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IgnoreProductionExceptionHandlerTest {

  private static final String IGNORE_PRODUCTION_EXCEPTION_CLASSES = "ignore.production.exception.classes";
  private static final String TOPIC = "test-topic";
  private static final byte[] KEY = new byte[]{0, 1, 2, 3};
  private static final byte[] VALUE = new byte[]{0, 1, 2, 3};

  @Test
  public void failWithoutConfiguredException() {
    IgnoreProductionExceptionHandler handler = new IgnoreProductionExceptionHandler();

    ProducerRecord<byte[], byte[]> record = new ProducerRecord(TOPIC, KEY, VALUE);
    Exception exception = new RecordTooLargeException();

    ProductionExceptionHandler.ProductionExceptionHandlerResponse response = handler.handle(record, exception);
    assertEquals(response, ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL);
  }

  @Test
  public void continueWithConfiguredException() {
    Map<String, String> configs = new HashMap<>();
    configs.put(IGNORE_PRODUCTION_EXCEPTION_CLASSES, "org.apache.kafka.common.errors.RecordTooLargeException");

    IgnoreProductionExceptionHandler handler = new IgnoreProductionExceptionHandler();
    handler.configure(configs);

    ProducerRecord<byte[], byte[]> record = new ProducerRecord(TOPIC, KEY, VALUE);
    Exception exception = new RecordTooLargeException();

    ProductionExceptionHandler.ProductionExceptionHandlerResponse response = handler.handle(record, exception);
    assertEquals(response, ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE);
  }

  @Test
  public void failWithConfiguredException() {
    Map<String, String> configs = new HashMap<>();
    configs.put(IGNORE_PRODUCTION_EXCEPTION_CLASSES, "org.apache.kafka.common.errors.RecordBatchTooLargeException");

    IgnoreProductionExceptionHandler handler = new IgnoreProductionExceptionHandler();
    handler.configure(configs);

    ProducerRecord<byte[], byte[]> record = new ProducerRecord(TOPIC, KEY, VALUE);
    Exception exception = new RecordTooLargeException();

    ProductionExceptionHandler.ProductionExceptionHandlerResponse response = handler.handle(record, exception);
    assertEquals(response, ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL);
  }

  @Test
  public void continueWithConfiguredMultipleExceptions() {
    Map<String, String> configs = new HashMap<>();
    configs.put(IGNORE_PRODUCTION_EXCEPTION_CLASSES, "org.apache.kafka.common.errors.ProducerFencedException,org.apache.kafka.common.errors.RecordTooLargeException");

    IgnoreProductionExceptionHandler handler = new IgnoreProductionExceptionHandler();
    handler.configure(configs);

    ProducerRecord<byte[], byte[]> record = new ProducerRecord(TOPIC, KEY, VALUE);
    Exception exception = new RecordTooLargeException();

    ProductionExceptionHandler.ProductionExceptionHandlerResponse response = handler.handle(record, exception);
    assertEquals(response, ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE);
  }

  @Test
  public void failWithConfiguredMultipleExceptions() {
    Map<String, String> configs = new HashMap<>();
    configs.put(IGNORE_PRODUCTION_EXCEPTION_CLASSES, "org.apache.kafka.common.errors.ProducerFencedException,org.apache.kafka.common.errors.RecordBatchTooLargeException");

    IgnoreProductionExceptionHandler handler = new IgnoreProductionExceptionHandler();
    handler.configure(configs);

    ProducerRecord<byte[], byte[]> record = new ProducerRecord(TOPIC, KEY, VALUE);
    Exception exception = new RecordTooLargeException();

    ProductionExceptionHandler.ProductionExceptionHandlerResponse response = handler.handle(record, exception);
    assertEquals(response, ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL);
  }
}
