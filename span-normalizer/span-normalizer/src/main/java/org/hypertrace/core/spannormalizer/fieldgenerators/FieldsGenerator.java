package org.hypertrace.core.spannormalizer.fieldgenerators;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.spannormalizer.util.AttributeValueCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FieldsGenerator {
  private static Logger LOGGER = LoggerFactory.getLogger(FieldsGenerator.class);

  private final Map<String, ProtocolFieldsGenerator> protocolFieldsGeneratorMap;
  private final HttpFieldsGenerator httpFieldsGenerator;
  private final GrpcFieldsGenerator grpcFieldsGenerator;
  private final SqlFieldsGenerator sqlFieldsGenerator;
  private final RpcFieldsGenerator rpcFieldsGenerator;

  public FieldsGenerator() {
    this.protocolFieldsGeneratorMap = new HashMap<>();
    this.httpFieldsGenerator = new HttpFieldsGenerator();
    this.grpcFieldsGenerator = new GrpcFieldsGenerator();
    this.sqlFieldsGenerator = new SqlFieldsGenerator();
    this.rpcFieldsGenerator = new RpcFieldsGenerator(this.grpcFieldsGenerator);
    initializeProtocolFieldGeneratorsMap();
  }

  private void initializeProtocolFieldGeneratorsMap() {
    httpFieldsGenerator
        .getProtocolCollectorSpanKeys()
        .forEach(k -> protocolFieldsGeneratorMap.put(k, httpFieldsGenerator));
    grpcFieldsGenerator
        .getProtocolCollectorSpanKeys()
        .forEach(k -> protocolFieldsGeneratorMap.put(k, grpcFieldsGenerator));
    sqlFieldsGenerator
        .getProtocolCollectorSpanKeys()
        .forEach(k -> protocolFieldsGeneratorMap.put(k, sqlFieldsGenerator));
    rpcFieldsGenerator
        .getProtocolCollectorSpanKeys()
        .forEach(k -> protocolFieldsGeneratorMap.put(k, rpcFieldsGenerator));
  }

  public void populateOtherFields(
      Event.Builder eventBuilder,
      Map<String, AttributeValue> attributeValueMap) {
    try {
      this.httpFieldsGenerator.populateOtherFields(eventBuilder);
      this.sqlFieldsGenerator.populateOtherFields(eventBuilder, attributeValueMap);
      this.grpcFieldsGenerator.populateOtherFields(eventBuilder, attributeValueMap);
    } catch (Exception ex) {
      LOGGER.error("An error occurred while populating other fields: %s", ex);
    }
  }

  /**
   * Expectations: 1. key to be in lower case and to correspond to the key in KeyValue. 2.
   * eventBuilder.getAttributesBuilder().getAttributeMap() should be non null.
   *
   * @param key
   * @param keyValue
   * @param eventBuilder
   * @param tagsMap
   */
  public void addValueToBuilder(
      String key,
      JaegerSpanInternalModel.KeyValue keyValue,
      Event.Builder eventBuilder,
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    try {
      ProtocolFieldsGenerator protocolFieldsGenerator = protocolFieldsGeneratorMap.get(key);
      if (protocolFieldsGenerator != null) {
        protocolFieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap);
      } else {
        // If no ProtocolFieldsGenerator to handle that key, try some of the custom conversion code
        // in the various
        // converters eg. HttpFieldsGenerator to handle keys that start with a key - headers, params
        if (!httpFieldsGenerator.handleStartsWithKeyIfNecessary(key, keyValue, eventBuilder) &&
            !rpcFieldsGenerator.handleKeyIfNecessary(key, keyValue, eventBuilder, tagsMap)) {
          // Key cannot be converted to any of the fields, put it in event attributes. This may be a
          // repeated
          // operation for some keys since in JaegerSpanToRawSpanAvroConverter.buildEvent(), we add
          // all the key values to
          // the events attributes. That is temporary once all event consumers use the new event
          // format we will remove
          // that logic and keys that cannot be converted into fields will be added to the event
          // builder attributes here.
          eventBuilder
              .getAttributesBuilder()
              .getAttributeMap()
              .put(key, AttributeValueCreator.createFromJaegerKeyValue(keyValue));
        }
      }
    } catch (Exception ex) {
      LOGGER.error(
          "An error occurred while converting span raw key: {} with key value: {}",
          key,
          keyValue,
          ex);
    }
  }
}
