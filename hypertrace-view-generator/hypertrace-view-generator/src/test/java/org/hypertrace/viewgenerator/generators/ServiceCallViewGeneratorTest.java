package org.hypertrace.viewgenerator.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.viewgenerator.api.ServiceCallView;
import org.hypertrace.viewgenerator.generators.ViewGeneratorState.TraceState;
import org.junit.jupiter.api.Test;

public class ServiceCallViewGeneratorTest {

  @Test
  public void testServiceCallViewGenerator_HotrodTrace() throws IOException {
    StructuredTrace trace = TestUtilities.getSampleHotRodTrace();
    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    ServiceCallViewGenerator serviceCallViewGenerator = new ServiceCallViewGenerator();
    List<ServiceCallView> individuallyComputedServiceCalls = Lists.newArrayList();
    individuallyComputedServiceCalls.addAll(
        verifyEdgesCreatedFromApiNodeEdge_HotrodTrace(
            apiTraceGraph, trace, Lists.newArrayList(), serviceCallViewGenerator));
    individuallyComputedServiceCalls.addAll(
        verifyEdgesCreatedFromRootEntryEvent_HotrodTrace(
            apiTraceGraph, trace, serviceCallViewGenerator));
    individuallyComputedServiceCalls.addAll(
        verifyEdgesCreatedFromEdgeFromBackend_HotrodTrace(
            apiTraceGraph, trace, serviceCallViewGenerator));
    individuallyComputedServiceCalls.addAll(
        verifyEdgesCreatedFromEdgeFromNonRootEntryEvent_HotrodTrace(
            apiTraceGraph, new TraceState(trace), trace, serviceCallViewGenerator));

    List<ServiceCallView> serviceCallViewRecords = serviceCallViewGenerator.process(trace);
    assertEquals(individuallyComputedServiceCalls, serviceCallViewRecords);
  }

  private List<ServiceCallView> verifyEdgesCreatedFromApiNodeEdge_HotrodTrace(
      ApiTraceGraph apiTraceGraph,
      StructuredTrace trace,
      List<ServiceCallView> serviceCallViewRecords,
      ServiceCallViewGenerator serviceCallViewGenerator) {
    serviceCallViewGenerator.createEdgeFromApiNodeEdge(
        apiTraceGraph, trace, serviceCallViewRecords);
    // there are 12 api node edges, which translates to 12 serviceCall records
    // frontend -> customer, 1
    // frontend -> driver, 1
    // frontend -> route, 10
    assertEquals(12, serviceCallViewRecords.size());
    Map<String, AtomicInteger> calleeCount = Maps.newHashMap();
    serviceCallViewRecords.forEach(
        v ->
            calleeCount
                .computeIfAbsent(v.getCalleeService(), c -> new AtomicInteger(0))
                .incrementAndGet());
    assertEquals(3, calleeCount.size());
    assertEquals(1, calleeCount.get("customer").get());
    assertEquals(1, calleeCount.get("driver").get());
    assertEquals(10, calleeCount.get("route").get());
    return serviceCallViewRecords;
  }

  private List<ServiceCallView> verifyEdgesCreatedFromRootEntryEvent_HotrodTrace(
      ApiTraceGraph apiTraceGraph,
      StructuredTrace trace,
      ServiceCallViewGenerator serviceCallViewGenerator) {
    List<ServiceCallView> serviceCallViewRecords = Lists.newArrayList();
    serviceCallViewGenerator.createEdgeFromRootEntryEvent(
        apiTraceGraph, trace, serviceCallViewRecords);
    // there is a single root event in the trace
    assertEquals(1, serviceCallViewRecords.size());
    return serviceCallViewRecords;
  }

  private List<ServiceCallView> verifyEdgesCreatedFromEdgeFromBackend_HotrodTrace(
      ApiTraceGraph apiTraceGraph,
      StructuredTrace trace,
      ServiceCallViewGenerator serviceCallViewGenerator) {
    List<ServiceCallView> serviceCallViewRecords = Lists.newArrayList();
    serviceCallViewGenerator.createEdgeFromBackend(apiTraceGraph, trace, serviceCallViewRecords);
    // in total 14 edges, representing calls to backend
    // customer -> mysql, 1
    // driver -> redis, 13
    Map<String, AtomicInteger> callerCount = Maps.newHashMap();
    serviceCallViewRecords.forEach(
        v ->
            callerCount
                .computeIfAbsent(v.getCallerService(), c -> new AtomicInteger(0))
                .incrementAndGet());
    assertEquals(2, callerCount.size());
    assertEquals(1, callerCount.get("customer").get());
    assertEquals(13, callerCount.get("driver").get());
    return serviceCallViewRecords;
  }

  private List<ServiceCallView> verifyEdgesCreatedFromEdgeFromNonRootEntryEvent_HotrodTrace(
      ApiTraceGraph apiTraceGraph,
      TraceState traceState,
      StructuredTrace trace,
      ServiceCallViewGenerator serviceCallViewGenerator) {
    List<ServiceCallView> serviceCallViewRecords = Lists.newArrayList();
    serviceCallViewGenerator.createEdgeFromNonRootEntryEvent(
        apiTraceGraph,
        trace,
        traceState.getEventMap(),
        traceState.getChildToParentEventIds(),
        serviceCallViewRecords);
    assertEquals(0, serviceCallViewRecords.size());
    return serviceCallViewRecords;
  }
}
