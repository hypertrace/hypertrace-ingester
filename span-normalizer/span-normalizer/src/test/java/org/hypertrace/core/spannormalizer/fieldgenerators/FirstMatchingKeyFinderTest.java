package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.spannormalizer.utils.TestUtils.createKeyValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FirstMatchingKeyFinderTest {
  @Test
  public void testStringValueByFirstMatchingKey() {
    Optional<String> stringVal =
        FirstMatchingKeyFinder.getStringValueByFirstMatchingKey(
            Map.of(
                "a1", createKeyValue("v1"), "a2", createKeyValue("v2"), "a3", createKeyValue("v3")),
            List.of("a2", "a3", "a4"));

    Assertions.assertEquals("v2", stringVal.get());

    // Takes a validation predicate
    stringVal =
        FirstMatchingKeyFinder.getStringValueByFirstMatchingKey(
            Map.of(
                "a1", createKeyValue("v1"), "a2", createKeyValue("v2"), "a3", createKeyValue("v3")),
            List.of("a2", "a3", "a4"),
            v -> !v.equals("v2"));

    Assertions.assertEquals("v3", stringVal.get());

    // No valid value found
    stringVal =
        FirstMatchingKeyFinder.getStringValueByFirstMatchingKey(
            Map.of(
                "a1", createKeyValue("v1"), "a2", createKeyValue("v2"), "a3", createKeyValue("v3")),
            List.of("a2", "a4"),
            v -> !v.equals("v2"));

    Assertions.assertTrue(stringVal.isEmpty());

    // None of the keys match
    stringVal =
        FirstMatchingKeyFinder.getStringValueByFirstMatchingKey(
            Map.of(
                "a1", createKeyValue("v1"), "a2", createKeyValue("v2"), "a3", createKeyValue("v3")),
            List.of("a5", "a4"));

    Assertions.assertTrue(stringVal.isEmpty());
  }

  @Test
  public void testIntegerValueByFirstMatchingKey() {
    Optional<Integer> intVal =
        FirstMatchingKeyFinder.getIntegerValueByFirstMatchingKey(
            Map.of("a1", createKeyValue(11), "a2", createKeyValue(12), "a3", createKeyValue(13)),
            List.of("a2", "a3", "a4"));

    Assertions.assertEquals(12, intVal.get().intValue());

    // No key matches
    intVal =
        FirstMatchingKeyFinder.getIntegerValueByFirstMatchingKey(
            Map.of("a1", createKeyValue(11), "a2", createKeyValue(12), "a3", createKeyValue(13)),
            List.of("a4", "a5"));

    Assertions.assertTrue(intVal.isEmpty());
  }
}
