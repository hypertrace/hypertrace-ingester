package org.hypertrace.core.rawspansgrouper.validator;

import java.util.Objects;
import org.hypertrace.core.spannormalizer.IpIdentity;
import org.hypertrace.core.spannormalizer.PeerIdentity;

public class PeerIdentityValidator {

  public static boolean isValid(PeerIdentity peerIdentity) {
    final IpIdentity ipIdentity = peerIdentity.getIpIdentity();
    if (Objects.nonNull(ipIdentity)) {
      return IpIdentityValidator.isValid(ipIdentity);
    }
    return true;
  }
}
