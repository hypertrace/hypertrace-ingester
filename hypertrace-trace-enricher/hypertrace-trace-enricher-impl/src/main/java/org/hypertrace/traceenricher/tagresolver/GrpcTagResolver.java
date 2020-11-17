package org.hypertrace.traceenricher.tagresolver;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;

public class GrpcTagResolver {

  private static final String OTEL_RPC_METHOD = "rpc.method";
  private static final String OTHER_GRPC_METHOD = "grpc.method";

  public static List<String> getTagsForGrpcMethod() {
    return Lists.newArrayList(Sets.newHashSet(OTEL_RPC_METHOD, OTHER_GRPC_METHOD));
  }


}
