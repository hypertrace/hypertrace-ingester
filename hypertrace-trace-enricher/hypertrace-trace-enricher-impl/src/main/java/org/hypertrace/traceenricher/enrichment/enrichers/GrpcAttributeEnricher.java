package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.GRPC_REQUEST_ENDPOINT;
import static org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants.GRPC_REQUEST_URL;

import java.util.Optional;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;

public class GrpcAttributeEnricher extends AbstractTraceEnricher {

  private static final String GRPC_RECV_DOT = "Recv.";
  private static final String GRPC_SENT_DOT = "Sent.";

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    // if protocol is Grpc update attribute
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    if (Protocol.PROTOCOL_GRPC == protocol) {
      Optional<String> grpcRequestEndpoint =
          RpcSemanticConventionUtils.getGrpcRequestEndpoint(event);
      if (grpcRequestEndpoint.isPresent()) {
        addEnrichedAttribute(
            event, GRPC_REQUEST_ENDPOINT, AttributeValueCreator.create(grpcRequestEndpoint.get()));

        String prefix = getPrefix(event);
        addEnrichedAttribute(
            event,
            GRPC_REQUEST_URL,
            AttributeValueCreator.create(prefix.concat(grpcRequestEndpoint.get())));
      }
    }
  }

  private String getPrefix(Event event) {
    if (EnrichedSpanUtils.isEntrySpan(event)) {
      return GRPC_RECV_DOT;
    } else if (EnrichedSpanUtils.isExitSpan(event)) {
      return GRPC_SENT_DOT;
    }
    return "";
  }
}
