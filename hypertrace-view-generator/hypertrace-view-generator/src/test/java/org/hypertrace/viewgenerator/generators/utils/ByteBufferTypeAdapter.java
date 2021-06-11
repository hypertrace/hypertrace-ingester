package org.hypertrace.viewgenerator.generators.utils;

import com.google.gson.*;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import org.apache.commons.codec.binary.Base64;

public class ByteBufferTypeAdapter
    implements JsonDeserializer<ByteBuffer>, JsonSerializer<ByteBuffer> {

  public ByteBuffer deserialize(
      JsonElement jsonElement, Type type, JsonDeserializationContext context) {
    return ByteBuffer.wrap(Base64.decodeBase64(jsonElement.getAsString()));
  }

  @Override
  public JsonElement serialize(ByteBuffer src, Type typeOfSrc, JsonSerializationContext context) {
    return new JsonPrimitive(Base64.encodeBase64String(src.array()));
  }
}
