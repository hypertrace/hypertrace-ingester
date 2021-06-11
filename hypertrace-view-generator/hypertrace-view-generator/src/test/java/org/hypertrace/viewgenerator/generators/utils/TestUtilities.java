package org.hypertrace.viewgenerator.generators.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URL;
import java.nio.ByteBuffer;
import org.hypertrace.core.datamodel.StructuredTrace;

public class TestUtilities {

  private static Gson gson =
      new GsonBuilder()
          .serializeNulls()
          .registerTypeHierarchyAdapter(ByteBuffer.class, new ByteBufferTypeAdapter())
          .create();

  public static StructuredTrace getSampleHotRodTrace() throws FileNotFoundException {
    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("StructuredTrace-Hotrod.json");
    return gson.fromJson(new FileReader(resource.getPath()), StructuredTrace.class);
  }
}
