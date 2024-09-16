package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.traceenricher.util.EnricherUtil.getResourceAttribute;

import com.typesafe.config.Config;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enricher to add resource attributes to the spans. As of now resource attributes are attached from
 * process tags.
 */
public class ResourceAttributeEnricher extends AbstractTraceEnricher {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceAttributeEnricher.class);
  private static final String RESOURCE_ATTRIBUTES_CONFIG_KEY = "attributes";
  private static final String NODE_SELECTOR_KEY = "node.selector";
  private static final String ATTRIBUTES_TO_MATCH_CONFIG_KEY = "attributesToMatch";
  private List<String> resourceAttributesToAdd = new ArrayList<>();
  private Map<String, String> resourceAttributeKeysToMatch = new HashMap<>();

  @Override
  public void init(Config enricherConfig, ClientRegistry clientRegistry) {
    resourceAttributesToAdd = enricherConfig.getStringList(RESOURCE_ATTRIBUTES_CONFIG_KEY);
    resourceAttributeKeysToMatch =
        enricherConfig.getConfig(ATTRIBUTES_TO_MATCH_CONFIG_KEY).entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().unwrapped().toString()));
  }

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    try {
      if (!isValidEvent(event)) {
        return;
      }
      Map<String, AttributeValue> attributeMap = event.getAttributes().getAttributeMap();
      for (String resourceAttributeKey : resourceAttributesToAdd) {
        String resourceAttributeKeyToMatch = resourceAttributeKey;
        if (resourceAttributeKeysToMatch.containsKey(resourceAttributeKey)) {
          resourceAttributeKeyToMatch = resourceAttributeKeysToMatch.get(resourceAttributeKey);
        }
        Optional<AttributeValue> resourceAttributeMaybe =
            getResourceAttribute(trace, event, resourceAttributeKeyToMatch);

        resourceAttributeMaybe.ifPresent(
            attributeValue ->
                attributeMap.computeIfAbsent(
                    resourceAttributeKey,
                    key -> {
                      if (resourceAttributeKey.equals(NODE_SELECTOR_KEY)) {
                        attributeValue.setValue(
                            attributeValue
                                .getValue()
                                .substring(attributeValue.getValue().lastIndexOf('/') + 1));
                      }
                      return attributeValue;
                    }));
      }
    } catch (Exception e) {
      LOGGER.error(
          "Exception while enriching event with resource attributes having event id: {}",
          event.getEventId(),
          e);
    }
  }

  private boolean isValidEvent(Event event) {
    return (event.getResourceIndex() >= 0)
        && (event.getAttributes() != null)
        && (event.getAttributes().getAttributeMap() != null);
  }
}
