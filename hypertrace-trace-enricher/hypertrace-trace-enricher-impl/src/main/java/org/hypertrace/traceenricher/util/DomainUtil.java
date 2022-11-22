package org.hypertrace.traceenricher.util;

import com.google.common.net.InternetDomainName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DomainUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(DomainUtil.class);

  public static String getPrimaryDomain(String url) {
    try {
      return InternetDomainName.from(url).topPrivateDomain().toString();
    } catch (Exception exception) {
      LOGGER.error("Error while extracting the primary domain from the url {} ", url, exception);
      return url;
    }
  }
}
