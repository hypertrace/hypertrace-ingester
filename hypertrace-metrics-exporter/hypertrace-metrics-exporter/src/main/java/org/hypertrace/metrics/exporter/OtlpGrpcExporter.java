package org.hypertrace.metrics.exporter;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc.MetricsServiceFutureStub;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.common.CompletableResultCode;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtlpGrpcExporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(OtlpGrpcExporter.class);

  private MetricsServiceFutureStub metricsService;
  private ManagedChannel managedChannel;
  private long timeoutNanos;

  public OtlpGrpcExporter(Config config) {
    String host = config.getString("host");
    int port = config.getInt("port");
    int timeOut = config.hasPath("timeout_nanos") ? config.getInt("timeout_nanos") : 0;

    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();

    managedChannel = channel;
    timeoutNanos = timeOut;
    metricsService = MetricsServiceGrpc.newFutureStub(channel);
  }

  public CompletableResultCode export(List<ResourceMetrics> metrics) {
    ExportMetricsServiceRequest exportMetricsServiceRequest =
        ExportMetricsServiceRequest.newBuilder().addAllResourceMetrics(metrics).build();

    final CompletableResultCode result = new CompletableResultCode();

    MetricsServiceFutureStub exporter;
    if (timeoutNanos > 0) {
      exporter = metricsService.withDeadlineAfter(timeoutNanos, TimeUnit.NANOSECONDS);
    } else {
      exporter = metricsService;
    }

    Futures.addCallback(
        exporter.export(exportMetricsServiceRequest),
        new FutureCallback<ExportMetricsServiceResponse>() {
          @Override
          public void onSuccess(@Nullable ExportMetricsServiceResponse response) {
            result.succeed();
          }

          @Override
          public void onFailure(Throwable t) {
            Status status = Status.fromThrowable(t);
            switch (status.getCode()) {
              case UNIMPLEMENTED:
                LOGGER.error("Failed to export metrics. Server responded with UNIMPLEMENTED. ", t);
                break;
              case UNAVAILABLE:
                LOGGER.error("Failed to export metrics. Server is UNAVAILABLE. ", t);
                break;
              default:
                LOGGER.warn("Failed to export metrics. Error message: " + t.getMessage());
                break;
            }
            result.fail();
          }
        },
        MoreExecutors.directExecutor());
    return result;
  }

  public void close() {
    try {
      managedChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Failed to shutdown the gRPC channel", e);
    }
  }
}
