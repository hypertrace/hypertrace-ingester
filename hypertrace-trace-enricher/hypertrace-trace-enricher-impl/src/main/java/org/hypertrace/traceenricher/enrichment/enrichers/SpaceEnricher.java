package org.hypertrace.traceenricher.enrichment.enrichers;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Space;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;

public class SpaceEnricher extends AbstractTraceEnricher {
  private static final String SPACE_IDS_ATTRIBUTE = EnrichedSpanConstants.getValue(Space.SPACE_IDS);

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    // TODO space generation will go here, once implemented
    addEnrichedAttribute(
        event, SPACE_IDS_ATTRIBUTE, AttributeValueCreator.create(Collections.emptyList()));
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
}