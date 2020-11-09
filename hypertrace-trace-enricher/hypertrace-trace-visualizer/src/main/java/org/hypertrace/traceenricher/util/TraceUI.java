package org.hypertrace.traceenricher.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * TraceUI converts a trace into a desired JSON format to be consumed by D3
 * It outputs the trace JSON files in /tmp/ directory. The trace json file names will
 * be `trace-id-in-hex`.json
 */
public class TraceUI {
  private final StructuredTrace trace;
  private final Set<JSONObject> roots;

  private TraceUI(StructuredTrace trace, Set<JSONObject> roots) {
    this.trace = trace;
    this.roots = roots;
  }

  public static TraceUI build(StructuredTrace trace) {
    StructuredTraceGraph graph = StructuredTraceGraph.createGraph(trace);
    Set<Event> roots = findRootEvents(trace);
    Set<JSONObject> rootJsons = new HashSet<>();
    for (Event root : roots) {
      JSONObject jsonObject = new JSONObject();
      rootJsons.add(jsonObject);

      Queue<Pair<Event, List<Integer>>> q = new LinkedList<>();
      q.add(Pair.of(root, new ArrayList<>()));
      jsonObject.put("id", HexUtils.getHex(root.getEventId()));
      jsonObject.put("name", root.getEventName());
      jsonObject.put("parent", "null");
      jsonObject.put("attributes", flattenAttributes(root));
      jsonObject.put("children", new JSONArray());

      while (!q.isEmpty()) {
        Pair<Event, List<Integer>> p = q.remove();
        Event e = p.getLeft();
        List<Integer> list = p.getRight();

        List<Event> children = graph.getChildrenEvents(e);
        if (children != null) {
          for (int i = 0; i < children.size(); i++) {
            Event child = children.get(i);
            JSONArray jsonArray = jsonObject.getJSONArray("children");
            for (Integer index : list) {
              JSONObject childJsonObject = jsonArray.getJSONObject(index);
              jsonArray = childJsonObject.getJSONArray("children");
            }
            JSONObject childJsonObject = new JSONObject();
            childJsonObject.put("id", HexUtils.getHex(child.getEventId()));
            childJsonObject.put("name", child.getEventName());
            childJsonObject.put("parent", HexUtils.getHex(e.getEventId()));
            childJsonObject.put("attributes", flattenAttributes(child));
            childJsonObject.put("children", new JSONArray());
            jsonArray.put(childJsonObject);

            ArrayList<Integer> indexesList = new ArrayList<>(list);
            indexesList.add(i);
            Pair<Event, List<Integer>> pairForChild = Pair.of(child, indexesList);
            q.add(pairForChild);
          }
        }
      }
    }

    return new TraceUI(trace, rootJsons);
  }

  private static JSONObject flattenAttributes(Event e) {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("eventId", HexUtils.getHex(e.getEventId()));
    if (e.getAttributes() != null && e.getAttributes().getAttributeMap() != null) {
      Map<String, AttributeValue> map = e.getAttributes().getAttributeMap();
      for (String key : map.keySet()) {
        String value = map.get(key).getValue();
        jsonObject.put(key, value);
      }
    }

    if (e.getEnrichedAttributes() != null && e.getEnrichedAttributes().getAttributeMap() != null) {
      Map<String, AttributeValue> map = e.getEnrichedAttributes().getAttributeMap();
      for (String key : map.keySet()) {
        String value = map.get(key).getValue();
        jsonObject.put(key, value);
      }
    }
    return jsonObject;
  }

  private static Set<Event> findRootEvents(StructuredTrace trace) {
    Set<Event> roots = new HashSet<>();
    Map<ByteBuffer, Event> eventMap = trace.getEventList().stream().collect(
        Collectors.toMap(Event::getEventId, span -> span, (e1, e2) -> e1));
    for (Event e : trace.getEventList()) {
      if (e.getEventRefList().isEmpty()) {
        roots.add(e);
      } else {
        // Check if there are any spans whose parent is missing in the trace (broken trace case).
        e.getEventRefList().forEach(r -> {
          if (!eventMap.containsKey(r.getEventId())) {
            roots.add(e);
          }
        });
      }
    }
    return roots;
  }

  public void writeToFile(File file) {
    try (FileWriter writer = new FileWriter(file)) {
      for (JSONObject jsonObject : roots) {
        writer.write(jsonObject.toString());
        writer.write("\n");
      }

      System.out.println("Written the JSON representation of trace "
          + HexUtils.getHex(trace.getTraceId()) + " to file: "
          + file.toPath().toAbsolutePath().toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
