package org.hypertrace.viewgenerator.generators;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;

public class LogEventViewGenerator implements JavaCodeBasedViewGenerator<> {

  @Override
  public List process(SpecificRecord specificRecord) {
    return null;
  }

  @Override
  public String getViewName() {
    return ;
  }

  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public Class getViewClass() {
    return null;
  }
}
