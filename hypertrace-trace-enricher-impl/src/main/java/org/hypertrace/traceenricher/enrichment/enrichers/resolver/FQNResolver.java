package org.hypertrace.traceenricher.enrichment.enrichers.resolver;

import com.google.common.base.Joiner;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.entity.constants.v1.K8sEntityAttribute;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache.EntityCacheProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to resolve the FQN from the hostname
 */
public class FQNResolver {
  private static final Logger LOG = LoggerFactory.getLogger(FQNResolver.class);
  private static final Joiner DOT_JOINER = Joiner.on(".");
  private static final String CLUSTER_NAME_ATTR_NAME =
      EntityConstants.getValue(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME);
  private static final String SVC_CLUSTER_LOCAL_SUFFIX = ".svc.cluster.local";
  private final EntityCache entityCache;

  public FQNResolver(EdsClient edsClient) {
    this.entityCache = EntityCacheProvider.get(edsClient);
  }

  public String resolve(String hostname, Event span) {
    hostname = hostname.replace(SVC_CLUSTER_LOCAL_SUFFIX, "");
    String cluster = EnrichedSpanUtils.getClusterName(span);
    String namespace = EnrichedSpanUtils.getNamespaceName(span);
    if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(cluster)) {
      return hostname;
    }

    String[] hostParts = hostname.split("\\.");
    //If hostname is a simple string assume it belongs to the current namespace and cluster
    if (hostParts.length == 1) {
      return DOT_JOINER.join(hostname, namespace, cluster);
    }
    //If hostname already contains the namespace don't append it again
    if (hostParts.length == 2
        && namespaceExists(span.getCustomerId(), hostParts[1], cluster)) {
      return DOT_JOINER.join(hostname, cluster);
    }
    return hostname;
  }

  private boolean namespaceExists(String customerId, String namespaceName, String clusterName) {
    try {
      return entityCache.getNameToNamespaceEntityIdCache()
          .get(Pair.of(customerId, namespaceName))
          .stream()
          .anyMatch(entity -> {
            AttributeValue attributeValue = entity.getAttributesMap().get(CLUSTER_NAME_ATTR_NAME);
            return attributeValue != null
                && attributeValue.getValue().getString().equals(clusterName);
          });
    } catch (ExecutionException ex) {
      LOG.error("Error fetching namespace for customer:{} and name:{}", customerId, namespaceName);
    }
    return false;
  }

}
