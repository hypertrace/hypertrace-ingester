package org.hypertrace.core.rawspansgrouper;

import org.hypertrace.core.spannormalizer.PeerIdentity;

public class PeerIdentityValidator {

  public static boolean isValid(PeerIdentity peerIdentity) {
    return IpIdentityValidator.isValid(peerIdentity.getIpIdentity());
  }
}
