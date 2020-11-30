package org.hypertrace.core.kafkastreams.framework.timestampextractors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class UseWallclockTimeOnInvalidTimestamp implements TimestampExtractor {

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    long timestamp = record.timestamp();
    if (timestamp < 0) {
      timestamp = System.currentTimeMillis();
    }
    return timestamp;
  }
}
