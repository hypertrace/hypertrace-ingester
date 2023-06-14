package org.hypertrace.traceenricher.enrichment.clients;

import io.grpc.Channel;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.entity.data.service.client.EdsCacheClient;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;
import org.hypertrace.trace.accessor.entities.TraceEntityAccessor;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;
import org.hypertrace.traceenricher.enrichment.enrichers.cache.EntityCache;
import org.hypertrace.traceenricher.util.UserAgentParser;

public interface ClientRegistry {

  GrpcChannelRegistry getChannelRegistry();

  Channel getAttributeServiceChannel();

  Channel getEntityServiceChannel();

  Channel getConfigServiceChannel();

  TraceEntityAccessor getTraceEntityAccessor();

  TraceAttributeReader<StructuredTrace, Event> getAttributeReader();

  EdsCacheClient getEdsCacheClient();

  EntityDataClient getEntityDataClient();

  EntityQueryServiceBlockingStub getEntityQueryServiceClient();

  EntityCache getEntityCache();

  CachingAttributeClient getCachingAttributeClient();

  UserAgentParser getUserAgentParser();
}
