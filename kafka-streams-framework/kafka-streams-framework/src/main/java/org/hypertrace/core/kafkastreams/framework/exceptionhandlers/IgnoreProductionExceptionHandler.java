package org.hypertrace.core.kafkastreams.framework.exceptionhandlers;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class IgnoreProductionExceptionHandler implements ProductionExceptionHandler {
  private static final String IGNORE_PRODUCTION_EXCEPTION_CLASSES = "ignore.production.exception.classes";

  private static final Logger LOGGER = LoggerFactory.getLogger(IgnoreProductionExceptionHandler.class);

  private List<Class<Exception>> ignoreExceptionClasses = new ArrayList<>();

  @Override
  public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
    for (Class<Exception> exceptionClass : ignoreExceptionClasses) {
      if (exceptionClass.isInstance(exception)) {
        LOGGER.error("Failed to produce record to topic={}, partition={}, size={} due to exception {}. will skip this record.",
          record.topic(), record.partition(), record.value().length, exception.getLocalizedMessage());
        return ProductionExceptionHandlerResponse.CONTINUE;
      }
    }
    return ProductionExceptionHandlerResponse.FAIL;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configs.containsKey(IGNORE_PRODUCTION_EXCEPTION_CLASSES)) {
      Object configValue = configs.get(IGNORE_PRODUCTION_EXCEPTION_CLASSES);
      if (configValue instanceof String) {
        LOGGER.info("found {}={}", IGNORE_PRODUCTION_EXCEPTION_CLASSES, configValue);
        List<String> classNameList = Arrays.asList(((String) configValue).trim().split("\\s*,\\s*"));
        for (String className : classNameList) {
          try {
            ignoreExceptionClasses.add((Class<Exception>) Class.forName(className));
          } catch (ClassNotFoundException e) {
            LOGGER.error("Class with name {} not found.", className);
          }
        }
      }
    }
  }
}
