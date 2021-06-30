package org.hypertrace.semantic.convention.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.Optional;
import javax.annotation.Nonnull;

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
  public static Optional<String> sanitizePath(@Nonnull String path) {
    return path.isBlank()
        ? Optional.empty()
        : Optional.ofNullable(DOT_JOINER.join(SLASH_SPLITTER.split(path)));
  }
}
