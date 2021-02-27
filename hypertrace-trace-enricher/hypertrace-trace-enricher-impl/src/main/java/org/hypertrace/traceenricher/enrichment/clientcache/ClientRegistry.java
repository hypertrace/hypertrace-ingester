package org.hypertrace.traceenricher.enrichment.clientcache;

import io.grpc.Channel;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.client.EdsClient;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;
import org.hypertrace.trace.reader.entities.TraceEntityReader;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;

public interface ClientRegistry {

  Channel getAttributeServiceChannel();

  Channel getEntityServiceChannel();

  Channel getConfigServiceChannel();

  TraceEntityReader<StructuredTrace, Event> getEntityReader();

  TraceAttributeReader<StructuredTrace, Event> getAttributeReader();

  EdsClient getEdsClient();

  EntityCache getEntityCache();
}
