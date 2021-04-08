package org.hypertrace.semantic.convention.utils.error;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.semantic.convention.constants.error.OTelErrorSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Error;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.semantic.convention.utils.SemanticConventionTestUtil;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ErrorSemanticConventionUtils} */
public class ErrorSemanticConventionUtilsTest {

  @Test
  public void testCheckForError() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Error.ERROR_ERROR),
                SemanticConventionTestUtil.buildAttributeValue("false")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = ErrorSemanticConventionUtils.checkForError(e);
    assertFalse(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Error.ERROR_ERROR),
                SemanticConventionTestUtil.buildAttributeValue("false")));
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorSemanticConventionUtils.checkForError(e);
    assertTrue(v);

    attributes = SemanticConventionTestUtil.buildAttributes(Map.of());
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorSemanticConventionUtils.checkForError(e);
    assertFalse(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(OTSpanTag.OT_SPAN_TAG_ERROR),
                SemanticConventionTestUtil.buildAttributeValue("true")));
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorSemanticConventionUtils.checkForError(e);
    assertTrue(v);
  }

  @Test
  public void testCheckForException() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelErrorSemanticConventions.EXCEPTION_TYPE.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("xyzerror")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v  = ErrorSemanticConventionUtils.checkForException(e);
    assertTrue(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelErrorSemanticConventions.EXCEPTION_MESSAGE.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("xyzerror")));
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorSemanticConventionUtils.checkForException(e);
    assertTrue(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of("other_error", SemanticConventionTestUtil.buildAttributeValue("xyzerror")));
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorSemanticConventionUtils.checkForException(e);
    assertFalse(v);
  }

  @Test
  public void testCheckForErrorStacktrace() {
    Event e = mock(Event.class);
    Attributes attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                RawSpanConstants.getValue(Error.ERROR_STACK_TRACE),
                SemanticConventionTestUtil.buildAttributeValue("org.abc.etc...")));
    when(e.getAttributes()).thenReturn(attributes);
    boolean v = ErrorSemanticConventionUtils.checkForErrorStackTrace(e);
    assertTrue(v);

    attributes =
        SemanticConventionTestUtil.buildAttributes(
            Map.of(
                OTelErrorSemanticConventions.EXCEPTION_STACKTRACE.getValue(),
                SemanticConventionTestUtil.buildAttributeValue("org.abc.etc...")));
    when(e.getAttributes()).thenReturn(attributes);
    v = ErrorSemanticConventionUtils.checkForErrorStackTrace(e);
    assertTrue(v);
  }
}
