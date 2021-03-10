package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.normalizer.constants.OTelRpcSystem.OTEL_RPC_SYSTEM_GRPC;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_METHOD;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SERVICE;
import static org.hypertrace.core.span.normalizer.constants.OTelSpanTag.OTEL_SPAN_TAG_RPC_SYSTEM;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_RESPONSE_METADATA;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.grpc.Grpc;
import org.hypertrace.core.datamodel.eventfields.rpc.Rpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcFieldsGenerator extends ProtocolFieldsGenerator<Rpc.Builder> {
  private static final String OTEL_SPAN_TAG_RPC_SYSTEM_ATTR = OTEL_SPAN_TAG_RPC_SYSTEM.getValue();
  private static final String OTEL_RPC_SYSTEM_GRPC_VAL = OTEL_RPC_SYSTEM_GRPC.getValue();
  private static final char DOT = '.';
  private static final String REQUEST_METADATA_PREFIX = RPC_REQUEST_METADATA.getValue() + DOT;
  private static final String RESPONSE_METADATA_PREFIX = RPC_RESPONSE_METADATA.getValue() + DOT;
  private static Logger LOGGER = LoggerFactory.getLogger(RpcFieldsGenerator.class);
  private static Map<String, FieldGenerator<Rpc.Builder>> fieldGeneratorMap =
      initializeFieldGenerators();

  GrpcFieldsGenerator grpcFieldsGenerator;

  public RpcFieldsGenerator(GrpcFieldsGenerator grpcFieldsGenerator) {
    this.grpcFieldsGenerator = grpcFieldsGenerator;
  }

  private static Map<String, FieldGenerator<Rpc.Builder>> initializeFieldGenerators() {
    Map<String, FieldGenerator<Rpc.Builder>> fieldGeneratorMap = new HashMap<>();
    fieldGeneratorMap.put(
        OTEL_SPAN_TAG_RPC_SYSTEM_ATTR,
        (key, keyValue, builder, tagsMap) -> {
          builder.setSystem(ValueConverter.getString(keyValue));
        });
    fieldGeneratorMap.put(
        OTEL_SPAN_TAG_RPC_SERVICE.getValue(),
        (key, keyValue, builder, tagsMap) -> {
          builder.setService(ValueConverter.getString(keyValue));
        });
    fieldGeneratorMap.put(
        OTEL_SPAN_TAG_RPC_METHOD.getValue(),
        (key, keyValue, builder, tagsMap) -> {
          builder.setMethod(ValueConverter.getString(keyValue));
        });

    return fieldGeneratorMap;
  }

  @Override
  protected Rpc.Builder getProtocolBuilder(Event.Builder eventBuilder) {
    return eventBuilder.getRpcBuilder();
  }

  @Override
  protected Map<String, FieldGenerator<Rpc.Builder>> getFieldGeneratorMap() {
    return fieldGeneratorMap;
  }

  public boolean handleKeyIfNecessary(
      String key,
      JaegerSpanInternalModel.KeyValue keyValue,
      Event.Builder eventBuilder,
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    String rpcSystem = getRpcSystem(tagsMap);
    if (!isRpcSystemGrpc(rpcSystem)) {
      return false;
    }
    Grpc.Builder grpcProtocolBuilder = grpcFieldsGenerator.getProtocolBuilder(eventBuilder);
    Map<String, FieldGenerator<Grpc.Builder>> rpcFieldGeneratorMap =
        grpcFieldsGenerator.getRpcFieldGeneratorMap();
    FieldGenerator<Grpc.Builder> rpcFieldGenerator = rpcFieldGeneratorMap.get(key);
    if (rpcFieldGenerator != null) {
      rpcFieldGenerator.run(key, keyValue, grpcProtocolBuilder, tagsMap);
      return true;
    }
    if (key.startsWith(REQUEST_METADATA_PREFIX)) {
      getSuffix(key, REQUEST_METADATA_PREFIX)
          .ifPresent(
              metadata ->
                  grpcFieldsGenerator.handleRpcRequestMetadata(
                      metadata, keyValue, grpcProtocolBuilder));
      return true;
    } else if (key.startsWith(RESPONSE_METADATA_PREFIX)) {
      getSuffix(key, RESPONSE_METADATA_PREFIX)
          .ifPresent(
              metadata ->
                  grpcFieldsGenerator.handleRpcResponseMetadata(
                      metadata, keyValue, grpcProtocolBuilder));
      return true;
    }
    return false;
  }

  private String getRpcSystem(Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    JaegerSpanInternalModel.KeyValue keyValue = tagsMap.get(OTEL_SPAN_TAG_RPC_SYSTEM_ATTR);
    if (keyValue != null) {
      String val = ValueConverter.getString(keyValue);
      return val;
    }
    return null;
  }

  private boolean isRpcSystemGrpc(String rpcSystem) {
    return (StringUtils.isNotBlank(rpcSystem)
        && StringUtils.equals(rpcSystem, OTEL_RPC_SYSTEM_GRPC_VAL));
  }

  private Optional<String> getSuffix(String key, String prefix) {
    if (key.startsWith(prefix) && key.length() > prefix.length()) {
      return Optional.of(key.substring(prefix.length()));
    }

    return Optional.empty();
  }
}
