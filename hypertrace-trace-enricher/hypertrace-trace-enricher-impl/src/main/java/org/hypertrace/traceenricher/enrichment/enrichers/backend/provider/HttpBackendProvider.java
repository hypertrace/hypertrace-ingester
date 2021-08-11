package org.hypertrace.traceenricher.enrichment.enrichers.backend.provider;

import static org.hypertrace.traceenricher.util.EnricherUtil.createAttributeValue;
import static org.hypertrace.traceenricher.util.EnricherUtil.getAttributesForFirstExistingKey;

import com.google.common.base.Suppliers;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.semantic.convention.utils.http.HttpSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;

public class HttpBackendProvider implements BackendProvider {
  private static final String COLON = ":";
  private static final int DEFAULT_HTTP_PORT = 80;
  private static final int DEFAULT_HTTPS_PORT = 443;
  private Supplier<Protocol> protocolSupplier;

  @Override
  public void init(Event event) {
    this.protocolSupplier = Suppliers.memoize(() -> EnrichedSpanUtils.getProtocol(event))::get;
  }

  @Override
  public boolean isValidBackend(Event event) {
    Protocol protocol = getProtocol();
    return protocol == Protocol.PROTOCOL_HTTP || protocol == Protocol.PROTOCOL_HTTPS;
  }

  @Override
  public BackendType getBackendType(Event event) {
    Protocol protocol = getProtocol();
    return protocol == Protocol.PROTOCOL_HTTPS ? BackendType.HTTPS : BackendType.HTTP;
  }

  @Override
  public Optional<String> getBackendUri(Event event, StructuredTraceGraph structuredTraceGraph) {
    Optional<String> httpHost = HttpSemanticConventionUtils.getHttpHost(event);
    // since http protocol has default ports for http and https protocol,
    // Removing default port if available as suffix in httpHost based on protocol
    if (httpHost.isPresent()) {
      String[] hostAndPort = httpHost.get().split(COLON);
      String host = hostAndPort[0];
      int port = hostAndPort.length == 2 ? Integer.valueOf(hostAndPort[1]) : getDefaultPort();
      if (port == getDefaultPort()) {
        return Optional.of(host);
      }
    }
    return httpHost;
  }

  @Override
  public Map<String, AttributeValue> getEntityAttributes(Event event) {
    Map<String, AttributeValue> entityAttributes = new HashMap<>();

    String path = HttpSemanticConventionUtils.getHttpPath(event).orElse(null);
    if (StringUtils.isNotEmpty(path)) {
      entityAttributes.put(
          EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH),
          createAttributeValue(path));
    }
    entityAttributes.putAll(
        getAttributesForFirstExistingKey(
            event, HttpSemanticConventionUtils.getAttributeKeysForHttpMethod()));
    entityAttributes.putAll(
        getAttributesForFirstExistingKey(
            event, HttpSemanticConventionUtils.getAttributeKeysForHttpRequestMethod()));

    return Collections.unmodifiableMap(entityAttributes);
  }

  @Override
  public Optional<String> getBackendOperation(Event event) {
    return HttpSemanticConventionUtils.getHttpMethod(event);
  }

  @Override
  public Optional<String> getBackendDestination(Event event) {
    return HttpSemanticConventionUtils.getHttpPath(event).filter(StringUtils::isNotEmpty);
  }

  private Protocol getProtocol() {
    return this.protocolSupplier.get();
  }

  private int getDefaultPort() {
    return getProtocol() == Protocol.PROTOCOL_HTTP ? DEFAULT_HTTP_PORT : DEFAULT_HTTPS_PORT;
  }
}
