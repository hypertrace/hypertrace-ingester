package org.hypertrace.viewgenerator.generators;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.viewgenerator.api.RawTraceView;
import org.hypertrace.viewgenerator.api.SpanEventView;
import org.hypertrace.viewgenerator.generators.ViewGeneratorState.TraceState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
public class RawTraceViewGeneratorTest {
    private RawTraceViewGenerator rawTraceViewGenerator;
    @BeforeEach
    public void setup() {
        rawTraceViewGenerator = new RawTraceViewGenerator();
    }
    @Test
    public void testGenerateView_HotrodTrace() throws IOException {
        URL resource =
                Thread.currentThread().getContextClassLoader().getResource("StructuredTrace-Hotrod.avro");
        SpecificDatumReader<StructuredTrace> datumReader =
                new SpecificDatumReader<>(StructuredTrace.getClassSchema());
        DataFileReader<StructuredTrace> dfrStructuredTrace =
                new DataFileReader<>(new File(resource.getPath()), datumReader);
        StructuredTrace trace = dfrStructuredTrace.next();
        dfrStructuredTrace.close();
        RawTraceViewGenerator rawTraceViewGenerator = new RawTraceViewGenerator();
        List<RawTraceView> rawTraceViews = rawTraceViewGenerator.process(trace);
        assertEquals(1,rawTraceViews.size());
        assertEquals(4,rawTraceViews.get(0).getNumServices());
        assertEquals(50,rawTraceViews.get(0).getNumSpans());
//        assertEquals(ByteBuffer.wrap("java.nio.HeapByteBuffer[pos=0 lim=44 cap=44]".getBytes("UTF-8")),rawTraceViews.get(0).getTraceId());
//
//        ByteBuffer bf = rawTraceViews.get(0).getTraceId();
//        bf.flip();
//        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(bf);
//        String text = charBuffer.toString();
//        System.out.println("UTF-8" + text);
//        assertEquals(text,"123");
    }
}
