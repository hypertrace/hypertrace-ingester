package org.hypertrace.semantic.convention.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Utils {

  private static final Splitter SLASH_SPLITTER = Splitter.on("/").omitEmptyStrings().trimResults();
  private static final Joiner DOT_JOINER = Joiner.on(".");

  /**
   * Takes in a param path and converts it to a DOT joined path /service.product/GetProducts ->
   * service.product.GetProducts
   *
   * @param path
   * @return
   */
  public @Nullable static String sanitizePath(@Nonnull String path) {
    if (path.isBlank()) {
      return null;
    }
    return DOT_JOINER.join(SLASH_SPLITTER.split(path));
  }
}
