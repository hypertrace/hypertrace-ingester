package org.hypertrace.telemetry.attribute.utils.error;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Error;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.telemetry.attribute.utils.AttributeTestUtil;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link ErrorTelemetryAttributeUtils}
 */
public class ErrorTelemetryAttributeUtilsTest {

  @Test
  public void testCheckForError() {
    Event e = mock(Event.class);
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Error.ERROR_ERROR),
            AttributeTestUtil.buildAttributeValue("xyzerror")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = ErrorTelemetryAttributeUtils.checkForError(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_ERROR),
            AttributeTestUtil.buildAttributeValue("xyzerror")));
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorTelemetryAttributeUtils.checkForError(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelErrorAttributes.EXCEPTION_TYPE.getValue(),
            AttributeTestUtil.buildAttributeValue("xyzerror")));
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorTelemetryAttributeUtils.checkForError(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            "other_error",
            AttributeTestUtil.buildAttributeValue("xyzerror")));
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorTelemetryAttributeUtils.checkForError(e);
    assertFalse(v);
  }

  @Test
  public void testCheckForErrorStacktrace() {
    Event e = mock(Event.class);
    Attributes attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            RawSpanConstants.getValue(Error.ERROR_STACK_TRACE),
            AttributeTestUtil.buildAttributeValue("org.abc.etc...")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = ErrorTelemetryAttributeUtils.checkForErrorStackTrace(e);
    assertTrue(v);

    attributes = AttributeTestUtil.buildAttributes(
        Map.of(
            OTelErrorAttributes.EXCEPTION_STACKTRACE.getValue(),
            AttributeTestUtil.buildAttributeValue("org.abc.etc...")));
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorTelemetryAttributeUtils.checkForErrorStackTrace(e);
    assertTrue(v);
  }
}
