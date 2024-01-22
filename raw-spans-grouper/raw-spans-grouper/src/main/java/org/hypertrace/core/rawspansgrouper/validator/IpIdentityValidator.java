package org.hypertrace.core.rawspansgrouper.validator;

import java.util.Objects;
import org.hypertrace.core.spannormalizer.IpIdentity;

public class IpIdentityValidator {
  public static boolean isValid(IpIdentity ipIdentity) {
    if (Objects.isNull(ipIdentity.getEnvironment()) || Objects.isNull(ipIdentity.getTenantId())) {
      return false;
    }

    return Objects.nonNull(ipIdentity.getHostAddr())
        || (Objects.nonNull(ipIdentity.getPeerAddr()) && Objects.nonNull(ipIdentity.getPeerPort()));
  }
}
