package org.hypertrace.viewgenerator.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.viewgenerator.api.BackendEntityView;
import org.junit.jupiter.api.Test;

public class BackendEntityViewGeneratorTest {

  @Test
  public void testBackendEntityViewGenerator_HotrodTrace() throws IOException {
    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("StructuredTrace-Hotrod.avro");
    SpecificDatumReader<StructuredTrace> datumReader =
        new SpecificDatumReader<>(StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace =
        new DataFileReader<>(new File(resource.getPath()), datumReader);
    StructuredTrace trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();
    BackendEntityViewGenerator backendEntityViewGenerator = new BackendEntityViewGenerator();
    List<BackendEntityView> backendEntityViews = backendEntityViewGenerator.process(trace);
    List<Event> computedBackendEvents = getEventsWithBackendEntity(trace);
    assertEntity(backendEntityViews, computedBackendEvents);
  }

  private List<Event> getEventsWithBackendEntity(StructuredTrace trace) {
    return trace.getEventList().stream()
        .filter(
            event ->
                event.getEnrichedAttributes().getAttributeMap().containsKey("BACKEND_ENTITY_ID"))
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private void assertEntity(List<BackendEntityView> backendViews, List<Event> backendEntity) {
    // asserting only for 1 element, as with sample trace, only 1 backend exists (redis)
    assertEquals(backendViews.size(), backendEntity.size());
    assertEquals(
        backendViews.get(0).getBackendId(),
        backendEntity
            .get(0)
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("BACKEND_ENTITY_ID")
            .getValue());
    assertEquals(
        backendViews.get(0).getBackendName(),
        backendEntity
            .get(0)
            .getEnrichedAttributes()
            .getAttributeMap()
            .get("BACKEND_ENTITY_NAME")
            .getValue());
  }
}
