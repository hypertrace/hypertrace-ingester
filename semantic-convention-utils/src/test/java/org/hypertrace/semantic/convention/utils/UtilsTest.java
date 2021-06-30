package org.hypertrace.semantic.convention.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class UtilsTest {

  @ParameterizedTest
  @ValueSource(strings = {" ", "   ", "\t", "\n"})
  public void testSanitizePathForEmptyStrings(String path) {
    assertNull(Utils.sanitizePath(path));
  }

  @ParameterizedTest
  @ValueSource(strings = {"samplepath", "sample_path", "sample path"})
  public void testSanitizePathForSanitizedStrings(String path) {
    assertEquals(path, Utils.sanitizePath(path));
  }

  @ParameterizedTest
  @MethodSource("getMapToTestSanitizedPath")
  public void testSanitizePathForUnsanitizedStrings(String path, String sanitized_path) {
    assertEquals(sanitized_path, Utils.sanitizePath(path));
  }

  private static Stream<Arguments> getMapToTestSanitizedPath() {
    return Stream.of(
        Arguments.arguments("a/b/c", "a.b.c"),
        Arguments.arguments("abc.wdwd/efwef/", "abc.wdwd.efwef"),
        Arguments.arguments("ffwrf/efwef/ffef", "ffwrf.efwef.ffef"));
  }
}
