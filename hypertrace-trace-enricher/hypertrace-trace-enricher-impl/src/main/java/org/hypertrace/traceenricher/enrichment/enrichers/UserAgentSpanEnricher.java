package org.hypertrace.traceenricher.enrichment.enrichers;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.semantic.convention.utils.http.HttpSemanticConventionUtils;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.UserAgent;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;

public class UserAgentSpanEnricher extends AbstractTraceEnricher {

  private static final String CACHE_CONFIG_KEY = "cache";
  private static final String CACHE_CONFIG_MAX_SIZE = "maxSize";
  public static final String CACHE_CONFIG_WRITE_EXPIRY_DURATION = "write.expire.duration";
  public static final String CACHE_CONFIG_REFRESH_EXPIRY_DURATION = "refresh.expire.duration";
  private static final int CACHE_MAX_SIZE_DEFAULT = 20000;
  private static final Duration DEFAULT_WRITE_EXPIRY_DURATION = Duration.ofMinutes(10);
  private static final Duration DEFAULT_REFRESH_EXPIRY_DURATION = Duration.ofMinutes(5);
  private static final String USER_AGENT_MAX_LENGTH_KEY = "user.agent.max.length";
  private static final int DEFAULT_USER_AGENT_MAX_LENGTH = 1000;
  private static final String DOT = ".";
  private final UserAgentStringParser userAgentStringParser =
      UADetectorServiceFactory.getResourceModuleParser();
  @Nullable private LoadingCache<String, ReadableUserAgent> userAgentCache;
  private int userAgentMaxLength;

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {
    if (enricherConfig.hasPath(CACHE_CONFIG_KEY)) {
      Config enricherCacheConfig = enricherConfig.getConfig(CACHE_CONFIG_KEY);
      int cacheSize =
          enricherCacheConfig.hasPath(CACHE_CONFIG_MAX_SIZE)
              ? enricherCacheConfig.getInt(CACHE_CONFIG_MAX_SIZE)
              : CACHE_MAX_SIZE_DEFAULT;
      Duration writeExpiryDuration =
          enricherCacheConfig.hasPath(CACHE_CONFIG_WRITE_EXPIRY_DURATION)
              ? enricherCacheConfig.getDuration(CACHE_CONFIG_WRITE_EXPIRY_DURATION)
              : DEFAULT_WRITE_EXPIRY_DURATION;
      Duration refreshExpiryDuration =
          enricherCacheConfig.hasPath(CACHE_CONFIG_REFRESH_EXPIRY_DURATION)
              ? enricherCacheConfig.getDuration(CACHE_CONFIG_REFRESH_EXPIRY_DURATION)
              : DEFAULT_REFRESH_EXPIRY_DURATION;

      userAgentCache =
          CacheBuilder.newBuilder()
              .maximumSize(cacheSize)
              .refreshAfterWrite(refreshExpiryDuration)
              .expireAfterWrite(writeExpiryDuration)
              .recordStats()
              .build(
                  new CacheLoader<>() {
                    @Override
                    @ParametersAreNonnullByDefault
                    public ReadableUserAgent load(String userAgentString) {
                      return userAgentStringParser.parse(userAgentString);
                    }
                  });
      PlatformMetricsRegistry.registerCache(
          this.getClass().getName() + DOT + "userAgentCache",
          userAgentCache,
          Collections.emptyMap());
    }
    if (enricherConfig.hasPath(USER_AGENT_MAX_LENGTH_KEY)) {
      userAgentMaxLength = enricherConfig.getInt(USER_AGENT_MAX_LENGTH_KEY);
    } else {
      userAgentMaxLength = DEFAULT_USER_AGENT_MAX_LENGTH;
    }
  }

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    if (event.getAttributes() == null) {
      return;
    }

    Map<String, AttributeValue> attributeMap = event.getAttributes().getAttributeMap();
    if (attributeMap == null) {
      return;
    }

    // extract the user-agent header
    Optional<String> mayBeUserAgent = getUserAgent(event);

    if (mayBeUserAgent.isPresent()) {
      String userAgentStr = mayBeUserAgent.get();
      if (userAgentStr.length() > userAgentMaxLength) {
        userAgentStr = userAgentStr.substring(0, userAgentMaxLength);
      }
      ReadableUserAgent userAgent =
          userAgentCache != null
              ? userAgentCache.getUnchecked(userAgentStr)
              : userAgentStringParser.parse(userAgentStr);
      addEnrichedAttribute(
          event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_NAME),
          AttributeValueCreator.create(userAgent.getName()));
      addEnrichedAttribute(
          event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_TYPE),
          AttributeValueCreator.create(userAgent.getType().getName()));
      addEnrichedAttribute(
          event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_DEVICE_CATEGORY),
          AttributeValueCreator.create(userAgent.getDeviceCategory().getName()));
      addEnrichedAttribute(
          event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_OS_NAME),
          AttributeValueCreator.create(userAgent.getOperatingSystem().getName()));
      addEnrichedAttribute(
          event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_OS_VERSION),
          AttributeValueCreator.create(
              userAgent.getOperatingSystem().getVersionNumber().toVersionString()));
      addEnrichedAttribute(
          event,
          EnrichedSpanConstants.getValue(UserAgent.USER_AGENT_BROWSER_VERSION),
          AttributeValueCreator.create(userAgent.getVersionNumber().toVersionString()));
    }
  }

  private Optional<String> getUserAgent(Event event) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    if (Protocol.PROTOCOL_HTTP == protocol || Protocol.PROTOCOL_HTTPS == protocol) {
      Optional<String> userAgent = HttpSemanticConventionUtils.getHttpUserAgentFromHeader(event);
      return userAgent.isPresent()
          ? userAgent
          : HttpSemanticConventionUtils.getHttpUserAgent(event);
    } else if (Protocol.PROTOCOL_GRPC == protocol) {
      return RpcSemanticConventionUtils.getGrpcUserAgent(event);
    }

    return Optional.empty();
  }
}
