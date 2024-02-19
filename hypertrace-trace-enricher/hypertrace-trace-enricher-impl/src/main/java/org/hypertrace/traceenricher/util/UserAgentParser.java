package org.hypertrace.traceenricher.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.Optional;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.semantic.convention.utils.http.HttpSemanticConventionUtils;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;

public class UserAgentParser {
  private static final String CACHE_CONFIG_KEY = "cache";
  private static final String CACHE_CONFIG_MAX_SIZE = "maxSize";
  public static final String CACHE_CONFIG_ACCESS_EXPIRATION_DURATION = "access.expire.duration";
  private static final String USER_AGENT_MAX_LENGTH_KEY = "max.length";
  private final UserAgentStringParser userAgentStringParser =
      UADetectorServiceFactory.getResourceModuleParser();

  private final LoadingCache<String, ReadableUserAgent> userAgentCache;
  private final int userAgentMaxLength;

  public UserAgentParser(Config config) {
    Config cacheConfig = config.getConfig(CACHE_CONFIG_KEY);
    this.userAgentMaxLength = config.getInt(USER_AGENT_MAX_LENGTH_KEY);
    int cacheMaxSize = cacheConfig.getInt(CACHE_CONFIG_MAX_SIZE);
    this.userAgentCache =
        CacheBuilder.newBuilder()
            .maximumSize(cacheMaxSize)
            .expireAfterAccess(cacheConfig.getDuration(CACHE_CONFIG_ACCESS_EXPIRATION_DURATION))
            .recordStats()
            .build(CacheLoader.from(userAgentStringParser::parse));
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        "userAgentCache", userAgentCache, Collections.emptyMap(), cacheMaxSize);
  }

  public Optional<ReadableUserAgent> getUserAgent(Event event) {
    return this.getUserAgentString(event)
        .map(
            userAgent ->
                userAgent.length() > userAgentMaxLength
                    ? userAgent.substring(0, userAgentMaxLength)
                    : userAgent)
        .map(this.userAgentCache::getUnchecked);
  }

  private Optional<String> getUserAgentString(Event event) {
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    if (Protocol.PROTOCOL_HTTP == protocol || Protocol.PROTOCOL_HTTPS == protocol) {
      return HttpSemanticConventionUtils.getHttpUserAgentFromHeader(event)
          .or(() -> HttpSemanticConventionUtils.getHttpUserAgent(event));
    }
    if (Protocol.PROTOCOL_GRPC == protocol) {
      return RpcSemanticConventionUtils.getGrpcUserAgent(event);
    }

    return Optional.empty();
  }
}
