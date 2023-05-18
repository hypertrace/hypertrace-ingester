package org.hypertrace.core.rawspansgrouper;

import com.google.protobuf.ByteString;
import io.vertx.core.AbstractVerticle;
import io.vertx.grpc.server.GrpcServer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.raw_span_grouper.GetSpansResponse;
import org.hypertrace.core.raw_span_grouper.SpanId;
import org.hypertrace.core.raw_span_grouper.SpanStoreGrpc;
import org.hypertrace.core.spannormalizer.SpanIdentity;

public class GrpcServerVerticle extends AbstractVerticle {

  private ReadOnlyKeyValueStore<SpanIdentity, RawSpan> spanStore;

  public GrpcServerVerticle(ReadOnlyKeyValueStore<SpanIdentity, RawSpan> spanStore) {
    this.spanStore = spanStore;
  }

  @Override
  public void start() {
    // Create the server
    GrpcServer rpcServer = GrpcServer.server(vertx);

    // The rpc service
    rpcServer.callHandler(
        SpanStoreGrpc.getGetSpansMethod(),
        request -> {
          request
              .last()
              .onSuccess(
                  msg -> {
                    System.out.println("Hello " + msg.getSpanIdentitiesCount());
                    SpanId spanId = msg.getSpanIdentities(0);
                    RawSpan span =
                        spanStore.get(
                            SpanIdentity.newBuilder()
                                .setSpanId(ByteBuffer.wrap(spanId.getSpanId().toByteArray()))
                                .setTraceId(ByteBuffer.wrap(spanId.getTraceId().toByteArray()))
                                .setTenantId(spanId.getTenantId())
                                .build());
                    try {
                      request
                          .response()
                          .end(
                              GetSpansResponse.newBuilder()
                                  .putAllSpans(Map.of("", ByteString.copyFrom(span.toByteBuffer())))
                                  .build());
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  });
        });

    // start the server
    vertx
        .createHttpServer()
        .requestHandler(rpcServer)
        .listen(8070)
        .onFailure(
            cause -> {
              cause.printStackTrace();
            });
  }
}
