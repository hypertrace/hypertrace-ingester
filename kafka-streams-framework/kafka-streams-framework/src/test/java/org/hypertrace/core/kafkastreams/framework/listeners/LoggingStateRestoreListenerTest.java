package org.hypertrace.core.kafkastreams.framework.listeners;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LoggingStateRestoreListenerTest {

  private LoggingStateRestoreListener underTest;

  @BeforeEach
  public void setUp() {
    underTest = new LoggingStateRestoreListener();
  }

  @Test
  public void whenRestoreStart_expectTotalToRestoreAndStartTimesToBeInitialized() {
    TopicPartition tp1 = new TopicPartition("topic-1", 0);
    TopicPartition tp2 = new TopicPartition("topic-1", 1);
    underTest.onRestoreStart(tp1, "store-1", 10, 100);
    underTest.onRestoreStart(tp2, "store-2", 20, 50);

    assertThat(underTest.getTotalToRestore().get(tp1), equalTo(90l));
    assertThat(underTest.getTotalToRestore().get(tp2), equalTo(30l));
  }

  @Test
  public void whenBatchRestored_expectCurrentProgressToBeCorrectlyComputed() {
    TopicPartition tp1 = new TopicPartition("topic-1", 0);
    underTest.onRestoreStart(tp1, "store-1", 10, 100);
    underTest.onBatchRestored(tp1, "store-1", -1, 20);

    assertThat(underTest.getRestoredSoFar().get(tp1), equalTo(20l));

    underTest.onBatchRestored(tp1, "store-1", -1, 30);
    assertThat(underTest.getRestoredSoFar().get(tp1), equalTo(50l));
  }

  @Test
  public void whenRestoreEnd_expectAllCachesToBeReset() {
    TopicPartition tp1 = new TopicPartition("topic-1", 0);
    underTest.onRestoreStart(tp1, "store-1", 10, 100);
    underTest.onBatchRestored(tp1, "store-1", -1, 20);
    underTest.onRestoreEnd(tp1, "store-1", 90);

    assertThat(underTest.getRestoredSoFar().get(tp1), nullValue());
    assertThat(underTest.getStartTimes().get(tp1), nullValue());
    assertThat(underTest.getTotalToRestore().get(tp1), nullValue());
  }
}