package org.hypertrace.traceenricher.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DomainUtilTest {

  @Test
  public void testGetDomainHost() {
    Assertions.assertEquals(DomainUtil.getPrimaryDomain("www.xyz.com"), "xyz.com");
    Assertions.assertEquals(DomainUtil.getPrimaryDomain("www.abc.xyz.com"), "xyz.com");
    Assertions.assertEquals(DomainUtil.getPrimaryDomain("abc.xyz.com"), "xyz.com");
    Assertions.assertEquals(DomainUtil.getPrimaryDomain("xyz.com"), "xyz.com");

    Assertions.assertEquals(DomainUtil.getPrimaryDomain("10.0.0.0"), "10.0.0.0");
  }
}
