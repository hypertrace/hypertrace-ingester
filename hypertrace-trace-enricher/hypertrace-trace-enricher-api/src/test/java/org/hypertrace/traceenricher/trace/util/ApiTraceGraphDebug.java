package org.hypertrace.traceenricher.trace.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;

/**
 * Meant for debugging / playing around with ApiTraceGraph
 *
 * The method {@link ApiTraceGraphDebug#debugApiTraceGraph()} prints important details of
 * ApiTraceGraph like list of ApiNodes (entry event, boundary events and exit events)
 * and details of the edges between ApiNode
 * This is helpful in understanding how we model apis in a Trace {@link StructuredTrace}
 */
public class ApiTraceGraphDebug {

  /**
   ApiTraceGraph for Hotrod trace
   ---
   Api Node Index 0
   Entry Event id: 383acc79e53b84da, service_name: frontend
   Events Count: 24
   id: 383acc79e53b84da, service_name: frontend
   id: 2af2bec0666abcb1, service_name: frontend
   id: 3e68fb176f4c336e, service_name: frontend
   id: 42b8e32573df3b0a, service_name: frontend
   id: 7507556b4659dbd5, service_name: frontend
   id: 5c6253a2b845b9ac, service_name: frontend
   id: 45ee254289410d2c, service_name: frontend
   id: 69a3ddd3e9939830, service_name: frontend
   id: 57628ef925adc6cc, service_name: frontend
   id: 3135f5a3080543fa, service_name: frontend
   id: 3cbdf4c1b4c1c8a1, service_name: frontend
   id: 3d4e5d9a96ddbb52, service_name: frontend
   id: 1d542ad0cefe15cb, service_name: frontend
   id: 49d2f1013807f2ca, service_name: frontend
   id: 633537306bb06ab7, service_name: frontend
   id: 0e00b6161d108e4f, service_name: frontend
   id: 2d9c1a3926fbba6c, service_name: frontend
   id: 7185c5a864902e6d, service_name: frontend
   id: 2db7e2b946f3412b, service_name: frontend
   id: 3484acf9dfcf5f75, service_name: frontend
   id: 6bea5062f49db48a, service_name: frontend
   id: 25ba455250f612c0, service_name: frontend
   id: 2e35bf7d6fb69be6, service_name: frontend
   id: 24ebcce46fa077b9, service_name: frontend
   Exit boundary Count: 12
   id: 3e68fb176f4c336e, service_name: frontend
   id: 49d2f1013807f2ca, service_name: frontend
   id: 633537306bb06ab7, service_name: frontend
   id: 0e00b6161d108e4f, service_name: frontend
   id: 2d9c1a3926fbba6c, service_name: frontend
   id: 7185c5a864902e6d, service_name: frontend
   id: 2db7e2b946f3412b, service_name: frontend
   id: 3484acf9dfcf5f75, service_name: frontend
   id: 6bea5062f49db48a, service_name: frontend
   id: 25ba455250f612c0, service_name: frontend
   id: 2e35bf7d6fb69be6, service_name: frontend
   id: 24ebcce46fa077b9, service_name: frontend
   ---
   Api Node Index 1
   Entry Event id: 1bdfaf5ac2b6f0d3, service_name: driver
   Events Count: 14
   id: 1bdfaf5ac2b6f0d3, service_name: driver
   id: 25cf40218aa42538, service_name: redis
   id: 422428b1ab769159, service_name: redis
   id: 23eb769e5de81a71, service_name: redis
   id: 4def061c540dbfb1, service_name: redis
   id: 39b478331ba03a60, service_name: redis
   id: 4cd63dc085980b7f, service_name: redis
   id: 788a166ed74cc9fe, service_name: redis
   id: 14f760c34d949159, service_name: redis
   id: 23db1eefebb27a0c, service_name: redis
   id: 7b2406a4fc59f0ef, service_name: redis
   id: 1a91cfccc1186bcc, service_name: redis
   id: 73b4ca0fedf207b8, service_name: redis
   id: 091aff0ab81bddae, service_name: redis
   Exit boundary Count: 13
   id: 25cf40218aa42538, service_name: redis
   id: 422428b1ab769159, service_name: redis
   id: 23eb769e5de81a71, service_name: redis
   id: 4def061c540dbfb1, service_name: redis
   id: 39b478331ba03a60, service_name: redis
   id: 4cd63dc085980b7f, service_name: redis
   id: 788a166ed74cc9fe, service_name: redis
   id: 14f760c34d949159, service_name: redis
   id: 23db1eefebb27a0c, service_name: redis
   id: 7b2406a4fc59f0ef, service_name: redis
   id: 1a91cfccc1186bcc, service_name: redis
   id: 73b4ca0fedf207b8, service_name: redis
   id: 091aff0ab81bddae, service_name: redis
   ---
   Api Node Index 2
   Entry Event id: 3aacfea7309a0605, service_name: customer
   Events Count: 2
   id: 3aacfea7309a0605, service_name: customer
   id: 4d13e597ae97d232, service_name: mysql
   Exit boundary Count: 1
   id: 4d13e597ae97d232, service_name: mysql
   ---
   Api Node Index 3
   Entry Event id: 4df5bf1db4508049, service_name: route
   Events Count: 1
   id: 4df5bf1db4508049, service_name: route
   Exit boundary Count: 0
   ---
   Api Node Index 4
   Entry Event id: 51093b5ee3ad58a8, service_name: route
   Events Count: 1
   id: 51093b5ee3ad58a8, service_name: route
   Exit boundary Count: 0
   ---
   Api Node Index 5
   Entry Event id: 3d4d74297f97c7f5, service_name: route
   Events Count: 1
   id: 3d4d74297f97c7f5, service_name: route
   Exit boundary Count: 0
   ---
   Api Node Index 6
   Entry Event id: 094dc03a19d69864, service_name: route
   Events Count: 1
   id: 094dc03a19d69864, service_name: route
   Exit boundary Count: 0
   ---
   Api Node Index 7
   Entry Event id: 3023972755b25ee7, service_name: route
   Events Count: 1
   id: 3023972755b25ee7, service_name: route
   Exit boundary Count: 0
   ---
   Api Node Index 8
   Entry Event id: 09e5688c857d918a, service_name: route
   Events Count: 1
   id: 09e5688c857d918a, service_name: route
   Exit boundary Count: 0
   ---
   Api Node Index 9
   Entry Event id: 3c91624cdb8d6a54, service_name: route
   Events Count: 1
   id: 3c91624cdb8d6a54, service_name: route
   Exit boundary Count: 0
   ---
   Api Node Index 10
   Entry Event id: 71cd8cdba7550d14, service_name: route
   Events Count: 1
   id: 71cd8cdba7550d14, service_name: route
   Exit boundary Count: 0
   ---
   Api Node Index 11
   Entry Event id: 161151419c2ce6b0, service_name: route
   Events Count: 1
   id: 161151419c2ce6b0, service_name: route
   Exit boundary Count: 0
   ---
   Api Node Index 12
   Entry Event id: 4301e4bbe15d1507, service_name: route
   Events Count: 1
   id: 4301e4bbe15d1507, service_name: route
   Exit boundary Count: 0
   ---
   Edge Index 0
   Api node index Src: 0, Tgt: 1
   Event Id Src: 3e68fb176f4c336e, Tgt: 1bdfaf5ac2b6f0d3
   ---
   Edge Index 1
   Api node index Src: 0, Tgt: 2
   Event Id Src: 49d2f1013807f2ca, Tgt: 3aacfea7309a0605
   ---
   Edge Index 2
   Api node index Src: 0, Tgt: 3
   Event Id Src: 633537306bb06ab7, Tgt: 4df5bf1db4508049
   ---
   Edge Index 3
   Api node index Src: 0, Tgt: 4
   Event Id Src: 0e00b6161d108e4f, Tgt: 51093b5ee3ad58a8
   ---
   Edge Index 4
   Api node index Src: 0, Tgt: 5
   Event Id Src: 2d9c1a3926fbba6c, Tgt: 3d4d74297f97c7f5
   ---
   Edge Index 5
   Api node index Src: 0, Tgt: 6
   Event Id Src: 7185c5a864902e6d, Tgt: 094dc03a19d69864
   ---
   Edge Index 6
   Api node index Src: 0, Tgt: 7
   Event Id Src: 2db7e2b946f3412b, Tgt: 3023972755b25ee7
   ---
   Edge Index 7
   Api node index Src: 0, Tgt: 8
   Event Id Src: 3484acf9dfcf5f75, Tgt: 09e5688c857d918a
   ---
   Edge Index 8
   Api node index Src: 0, Tgt: 9
   Event Id Src: 6bea5062f49db48a, Tgt: 3c91624cdb8d6a54
   ---
   Edge Index 9
   Api node index Src: 0, Tgt: 10
   Event Id Src: 25ba455250f612c0, Tgt: 71cd8cdba7550d14
   ---
   Edge Index 10
   Api node index Src: 0, Tgt: 11
   Event Id Src: 2e35bf7d6fb69be6, Tgt: 161151419c2ce6b0
   ---
   Edge Index 11
   Api node index Src: 0, Tgt: 12
   Event Id Src: 24ebcce46fa077b9, Tgt: 4301e4bbe15d1507
   ---
   */
  void debugApiTraceGraph() throws IOException {
    URL resource = Thread.currentThread().getContextClassLoader().
        getResource("StructuredTrace-Hotrod.avro");

    SpecificDatumReader<StructuredTrace> datumReader = new SpecificDatumReader<>(
        StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace = new DataFileReader<>(new File(resource.getPath()), datumReader);

    StructuredTrace trace = dfrStructuredTrace.next();
    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    apiTraceGraph.build();
    dfrStructuredTrace.close();

    AtomicInteger index = new AtomicInteger();
    apiTraceGraph.getNodeList().forEach(v -> {
      System.out.printf("Api Node Index %s %n", index.getAndIncrement());
      v.getEntryApiBoundaryEvent().ifPresent(w ->
          System.out.printf("Entry Event id: %s, service_name: %s %n", HexUtils.getHex(w.getEventId()), w.getServiceName()));
      System.out.printf("Events Count: %s %n", v.getEvents().size());
      v.getEvents().forEach(v1 -> System.out.printf("id: %s, service_name: %s %n", HexUtils.getHex(v1.getEventId()), v1.getServiceName()));
      System.out.println("Exit boundary Count: " + v.getExitApiBoundaryEvents().size());
      v.getExitApiBoundaryEvents().forEach(v1 -> System.out.printf("id: %s, service_name: %s %n", HexUtils.getHex(v1.getEventId()), v1.getServiceName()));
      System.out.println("---");
    });
    index.setRelease(0);
    apiTraceGraph.getApiNodeEventEdgeList().forEach(e -> {
      System.out.printf("Edge Index %s %n", index.getAndIncrement());
      System.out.printf("Api node index Src: %s, Tgt: %s %n", e.getSrcApiNodeIndex(), e.getTgtApiNodeIndex());
      System.out.printf("Event Id Src: %s, Tgt: %s %n",
          HexUtils.getHex(apiTraceGraph.getTrace().getEventList().get(e.getSrcEventIndex()).getEventId()),
          HexUtils.getHex(apiTraceGraph.getTrace().getEventList().get(e.getTgtEventIndex()).getEventId()));
      System.out.println("---");
    });
  }
}
