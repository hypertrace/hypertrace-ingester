package org.hypertrace.traceenricher.enrichment.enrichers.space;

import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.SPACE_IDS_ATTRIBUTE;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.data.service.client.EntityDataServiceClientProvider;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;

public class SpaceEnricher extends AbstractTraceEnricher {
  private SpaceRulesCachingClient ruleClient;
  private SpaceRuleEvaluator ruleEvaluator;

  @Override
  public void init(Config enricherConfig, EntityDataServiceClientProvider provider) {
    super.init(enricherConfig, provider);

    Channel configChannel =
        ManagedChannelBuilder.forAddress(
                enricherConfig.getString("config.service.config.host"),
                enricherConfig.getInt("config.service.config.port"))
            .usePlaintext()
            .build();

    Channel attributeChannel =
        ManagedChannelBuilder.forAddress(
                enricherConfig.getString("attribute.service.config.host"),
                enricherConfig.getInt("attribute.service.config.port"))
            .usePlaintext()
            .build();

    // TODO - we need a way to share caching clients like the attribute reader across enrichers
    this.init(
        new SpaceRulesCachingClient(configChannel),
        new SpaceRuleEvaluator(
            TraceAttributeReader.build(CachingAttributeClient.builder(attributeChannel).build())));
  }

  /**
   * The current design of enrichers with required no arg constructors does not allow mocking
   * dependencies, so exposing an init method to support mocking clients for tests
   */
  @VisibleForTesting
  void init(SpaceRulesCachingClient ruleClient, SpaceRuleEvaluator ruleEvaluator) {
    this.ruleClient = ruleClient;
    this.ruleEvaluator = ruleEvaluator;
  }

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    addEnrichedAttribute(
        event,
        SPACE_IDS_ATTRIBUTE,
        AttributeValueCreator.create(this.calculateSpaces(trace, event)));
  }

  @Override
  public void enrichTrace(StructuredTrace trace) {
    List<String> includedSpaceIds =
        trace.getEventList().stream()
            .map(EnrichedSpanUtils::getSpaceIds)
            .flatMap(Collection::stream)
            .distinct()
            .collect(Collectors.toList());

    trace
        .getAttributes()
        .getAttributeMap()
        .put(SPACE_IDS_ATTRIBUTE, AttributeValueCreator.create(includedSpaceIds));
  }

  private List<String> calculateSpaces(StructuredTrace trace, Event span) {
    return this.ruleClient.getRulesForTenant(span.getCustomerId()).stream()
        .map(rule -> this.ruleEvaluator.calculateSpacesForRule(trace, span, rule))
        .flatMap(Collection::stream)
        .distinct()
        .collect(Collectors.toList());
  }
}