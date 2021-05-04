package org.hypertrace.core.rawspansgrouper;

public class RawSpanGrouperConstants {
  public static final String INPUT_TOPIC_CONFIG_KEY = "input.topic";
  public static final String OUTPUT_TOPIC_CONFIG_KEY = "output.topic";
  public static final String SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY =
      "span.groupby.session.window.interval";
  public static final String RAW_SPANS_GROUPER_JOB_CONFIG = "raw-spans-grouper-job-config";
  public static final String SPAN_WINDOW_STORE = "span-window-store";
  public static final String TRACE_EMIT_TRIGGER_STORE = "trace-emit-trigger-store";
  public static final String OUTPUT_TOPIC_PRODUCER = "output-topic-producer";
  public static final String SPANS_PER_TRACE_METRIC = "spans_per_trace";
  public static final String TRACE_CREATION_TIME = "trace.creation.time";
  public static final String DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY =
      "dataflow.metriccollection.sampling.percent";
  public static final String INFLIGHT_TRACE_MAX_SPAN_COUNT = "max.span.count";
  public static final String DROPPED_SPANS_COUNTER = "hypertrace.dropped.spans";
  public static final String TRUNCATED_TRACES_COUNTER = "hypertrace.truncated.traces";
}
