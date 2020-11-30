package org.hypertrace.core.kafkastreams.framework.listeners;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingStateRestoreListener implements StateRestoreListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingStateRestoreListener.class);
  private final Map<TopicPartition, Long> totalToRestore = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> restoredSoFar = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Long> startTimes = new ConcurrentHashMap<>();

  @Override
  public void onRestoreStart(TopicPartition topicPartition, String store, long start, long end) {
    long toRestore = end - start;
    totalToRestore.put(topicPartition, toRestore);
    startTimes.put(topicPartition, System.currentTimeMillis());
    LOGGER
        .info("Starting restoration for [{}] on topic-partition [{}] total to restore [{}]", store,
            topicPartition, toRestore);
  }

  @Override
  public void onBatchRestored(TopicPartition topicPartition, String store, long start,
      long batchCompleted) {
    NumberFormat formatter = new DecimalFormat("#.##");

    long currentProgress = batchCompleted + restoredSoFar.getOrDefault(topicPartition, 0L);
    double percentComplete = (double) currentProgress / totalToRestore.get(topicPartition);

    LOGGER.info("Completed [{}] for [{}]% of total restoration for [{}] on [{}]",
        batchCompleted, formatter.format(percentComplete * 100.00), store, topicPartition);
    restoredSoFar.put(topicPartition, currentProgress);
  }

  @Override
  public void onRestoreEnd(TopicPartition topicPartition, String store, long totalRestored) {
    long startTs = startTimes.remove(topicPartition);
    LOGGER.info(
        "Restoration completed for [{}] on topic-partition [{}]. Total restored [{}] records. Duration [{}]",
        store, topicPartition, totalRestored,
        Duration.between(Instant.ofEpochMilli(startTs), Instant.now()));
    restoredSoFar.remove(topicPartition);
    totalToRestore.remove(topicPartition);
  }

  Map<TopicPartition, Long> getTotalToRestore() {
    return totalToRestore;
  }

  Map<TopicPartition, Long> getRestoredSoFar() {
    return restoredSoFar;
  }

  Map<TopicPartition, Long> getStartTimes() {
    return startTimes;
  }
}