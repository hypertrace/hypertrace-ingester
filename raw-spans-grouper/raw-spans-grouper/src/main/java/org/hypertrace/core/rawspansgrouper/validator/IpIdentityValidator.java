package org.hypertrace.core.rawspansgrouper.validator;

import java.util.Objects;
import org.hypertrace.core.spannormalizer.IpIdentity;

public class IpIdentityValidator {
  private static final String DEFAULT_ADDR_VALUE = "";
  private static final String DEFAULT_PORT_VALUE = "0";

  public static boolean isValid(IpIdentity ipIdentity) {
    if (Objects.isNull(ipIdentity.getEnvironment()) || Objects.isNull(ipIdentity.getTenantId())) {
      return false;
    }

    if (Objects.nonNull(ipIdentity.getHostAddr()) && Objects.nonNull(ipIdentity.getPeerAddr())) {
      return !ipIdentity.getHostAddr().equals(ipIdentity.getPeerAddr());
    }

    return (Objects.nonNull(ipIdentity.getHostAddr())
            && !DEFAULT_ADDR_VALUE.equals(ipIdentity.getHostAddr()))
        || (Objects.nonNull(ipIdentity.getPeerAddr())
            && !DEFAULT_ADDR_VALUE.equals(ipIdentity.getPeerAddr())
            && Objects.nonNull(ipIdentity.getPeerPort())
            && !DEFAULT_PORT_VALUE.equals(ipIdentity.getPeerPort()));
  }
}
