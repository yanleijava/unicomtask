/**
 *
 */
package io.streamnative.pulsar;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.common.stats.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarMirrorMakerStat {
    private static final Logger log = LoggerFactory.getLogger(PulsarMirrorMakerStat.class);
    static final Counter FLOWS = Counter.build()
            .name("mirror_flow_bytes").labelNames("taskId", "workId")
            .help("mirror maker flow total bytes").register();
    static final Counter TOPICFLOWS = Counter.build()
            .name("mirror_flow_topic_bytes").labelNames("taskId", "workId", "topic", "partition")
            .help("mirror maker flow topic total bytes")
            .register();

    static final Counter MSGCNT = Counter.build()
            .name("mirror_message_count").labelNames("taskId", "workId").help("mirror maker message count").register();
    static final Counter TOPICMSGCNT = Counter.build()
            .name("mirror_message_topic_count").labelNames("taskId", "workId", "topic", "partition")
            .help("mirror maker message topic count").register();
    static final Counter TOTALLATENCY = Counter.build()
            .name("mirror_message_total_latency").labelNames("taskId", "workId")
            .help("mirror maker total latency in milliseconds").register();
    static final Counter TOPICTOTALLATENCY = Counter.build()
            .name("mirror_message_topic_total_latency")
            .labelNames("taskId", "workId", "topic", "partition")
            .help("mirror maker topic total latency in milliseconds").register();

    static final Counter REBALANCENUM = Counter.build()
            .name("mirror_rebalance_num").labelNames("taskId", "workId").help("mirror maker rebalance num").register();
    static final Gauge LAGSIZE = Gauge.build()
            .name("mirror_consumer_lag")
            .labelNames("taskId", "workId", "topic", "partition", "consuemrgroup")
            .help("mirror consumer group lag size").register();
    static final Gauge PENDINGMESSAGELISTPUTNUM = Gauge.build()
            .name("pendingmessagelist_put_num")
            .labelNames("taskId", "workId", "topic", "consuemrgroup")
            .help("pending message list put num").register();
    static final Gauge PENDINGMESSAGELISTPOLLNUM = Gauge.build()
            .name("pendingmessagelist_poll_num")
            .labelNames("taskId", "workId", "topic", "consuemrgroup")
            .help("pending message list poll num").register();
    static final Gauge PENDINGMESSAGELISTCURRENTNUM = Gauge.build()
            .name("pendingmessagelist_current_num")
            .labelNames("taskId", "workId", "topic", "consuemrgroup")
            .help("pending message list current num").register();
    static final Gauge MIRRORMESSAGEAVGLATENCY = Gauge.build()
            .name("mirror_message_avg_latency")
            .labelNames("taskId", "workId", "topic", "partition")
            .help("mirror maker topic avg latency in milliseconds").register();

    //JVM metrics
    static final Gauge JVM_FGC_COUNT = Gauge.build().name("jvm_fgc_count").labelNames("taskId", "workId")
            .help("jvm full gc count").register();
    static final Gauge JVM_FGC_PAUSE = Gauge.build().name("jvm_fgc_pause").labelNames("taskId", "workId")
            .help("jvm full gc pause").register();
    static final Gauge JVM_ACCUMULATED_FGC_COUNT = Gauge.build().name("jvm_accumulated_fgc_count")
            .labelNames("taskId", "workId")
            .help("jvm accumulated full gc count").register();
    static final Gauge JVM_ACCUMULATED_FGC_PAUSE = Gauge.build().name("jvm_accumulated_fgc_pause")
            .labelNames("taskId", "workId")
            .help("jvm accumulated full gc pause").register();

    static final Gauge JVM_YGC_COUNT = Gauge.build().name("jvm_ygc_count").labelNames("taskId", "workId")
            .help("jvm young gc count").register();
    static final Gauge JVM_YGC_PAUSE = Gauge.build().name("jvm_ygc_pause").labelNames("taskId", "workId")
            .help("jvm young gc pause").register();
    static final Gauge JVM_ACCUMULATED_YGC_COUNT = Gauge.build().name("jvm_accumulated_ygc_count")
            .labelNames("taskId", "workId")
            .help("jvm accumulated young gc count").register();
    static final Gauge JVM_ACCUMULATED_YGC_PAUSE = Gauge.build().name("jvm_accumulated_ygc_pause")
            .labelNames("taskId", "workId")
            .help("jvm accumulated young gc pause").register();
    static final Gauge JVM_FULL_GC_COUNT = Gauge.build().name("jvm_full_gc_count")
            .labelNames("taskId", "workId")
            .help("jvm full gc count").register();
    static final Gauge JVM_FULL_GC_PAUSE = Gauge.build().name("jvm_full_gc_pause")
            .labelNames("taskId", "workId")
            .help("jvm full gc pause").register();
    static final Gauge JVM_HEAP_USED = Gauge.build().name("jvm_heap_used")
            .labelNames("taskId", "workId")
            .help("jvm heap used").register();
    static final Gauge JVM_MAX_DIRECT_MEMORY = Gauge.build().name("jvm_max_direct_memory")
            .labelNames("taskId", "workId")
            .help("jvm max direct memory").register();
    static final Gauge JVM_MAX_MEMORY = Gauge.build().name("jvm_max_memory")
            .labelNames("taskId", "workId")
            .help("jvm max memory").register();
    static final Gauge JVM_THREAD_CNT = Gauge.build().name("jvm_thread_cnt")
            .labelNames("taskId", "workId")
            .help("jvm thread count").register();
    static final Gauge JVM_TOTAL_MEMORY = Gauge.build().name("jvm_total_memory")
            .labelNames("taskId", "workId")
            .help("jvm total memory").register();
    static final Gauge TOOL_DEFAULT_POOL_ALLOCATED = Gauge.build().name("tool_default_pool_allocated")
            .labelNames("taskId", "workId")
            .help("tool default pool allocated").register();
    static final Gauge TOOL_DEFAULT_POOL_USED = Gauge.build().name("tool_default_pool_used")
            .labelNames("taskId", "workId")
            .help("tool default pool used").register();

    private String taskId;
    private String workId;

    public PulsarMirrorMakerStat(String taskId, String workId) {
        this.taskId = taskId;
        this.workId = workId;
    }

    public void updateJVMmetrics (List<Metrics> metrics) {
        for (Metrics metric : metrics) {
            try {
                Map<String, Object> m = metric.getMetrics();
                for (Map.Entry<String, Object> entry : m.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    if (key.endsWith("MarkSweep_accumulated_gc_count")) {
                        JVM_ACCUMULATED_FGC_COUNT.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("MarkSweep_accumulated_gc_pause")) {
                        JVM_ACCUMULATED_FGC_PAUSE.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("Scavenge_accumulated_gc_count")) {
                        JVM_ACCUMULATED_YGC_COUNT.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("Scavenge_accumulated_gc_pause")) {
                        JVM_ACCUMULATED_YGC_PAUSE.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("MarkSweep_gc_count")) {
                        JVM_FGC_COUNT.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("MarkSweep_gc_pause")) {
                        JVM_FGC_PAUSE.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("Scavenge_gc_count") || key.endsWith("jvm_gc_young_count")) {
                        JVM_YGC_COUNT.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("Scavenge_gc_pause") || key.endsWith("jvm_gc_young_pause")) {
                        JVM_YGC_PAUSE.labels(taskId, workId).set((long) value);
                    } else if (key.equals("jvm_full_gc_count") || key.equals("jvm_gc_old_count")) {
                        JVM_FULL_GC_COUNT.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("jvm_full_gc_pause") || key.equals("jvm_gc_old_pause")) {
                        JVM_FULL_GC_PAUSE.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("jvm_heap_used")) {
                        JVM_HEAP_USED.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("jvm_max_direct_memory")) {
                        JVM_MAX_DIRECT_MEMORY.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("jvm_max_memory")) {
                        JVM_MAX_MEMORY.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("jvm_thread_cnt")) {
                        JVM_THREAD_CNT.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("jvm_total_memory")) {
                        JVM_TOTAL_MEMORY.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("tool_default_pool_allocated")) {
                        TOOL_DEFAULT_POOL_ALLOCATED.labels(taskId, workId).set((long) value);
                    } else if (key.endsWith("tool_default_pool_used")) {
                        TOOL_DEFAULT_POOL_USED.labels(taskId, workId).set((long) value);
                    }
                }
            } catch (Exception e) {
                log.error("updateJVMmetrics error,", e);
            }
        }

    }

    public void recordBytes(long bytes) {
        if (bytes <= 0) {
            return;
        }
        FLOWS.labels(taskId, workId).inc(bytes);
    }

    public void recordTopicBytes(TopicPartition topicPartition, long bytes) {
        if (bytes <= 0) {
            return;
        }
        TOPICFLOWS.labels(
                taskId,
                workId,
                topicPartition.topic(),
                Long.toString(topicPartition.partition())
        ).inc(bytes);
    }

    public void recordMessageNum(long num) {
        MSGCNT.labels(taskId, workId).inc(num);
    }

    public void recordPendingMessageListStatus(String topic, String consumerGroup, int putNum, long pollNum,
                                               int currentNum) {
        PENDINGMESSAGELISTPOLLNUM.labels(taskId, workId, topic, consumerGroup).set(pollNum);
        PENDINGMESSAGELISTPUTNUM.labels(taskId, workId, topic, consumerGroup).set(putNum);
        PENDINGMESSAGELISTCURRENTNUM.labels(taskId, workId, topic, consumerGroup).set(currentNum);
    }

    public void recordMessageTopicNum(TopicPartition topicPartition, long num) {
        TOPICMSGCNT.labels(
                taskId,
                workId,
                topicPartition.topic(),
                Long.toString(topicPartition.partition())
        ).inc(num);

    }

    public void recordProcessLatency(long ms) {
        if (ms <= 0) {
            return;
        }
        TOTALLATENCY.labels(taskId, workId).inc(ms);
    }

    public void recordTopicProcessLatency(TopicPartition topicPartition, long ms) {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();
        TOPICTOTALLATENCY.labels(taskId, workId, topic, Long.toString(partition)).inc(ms);
        double avgLatency = TOPICTOTALLATENCY.labels(taskId, workId, topic, Long.toString(partition)).get()
                / TOPICMSGCNT.labels(taskId, workId, topic, Long.toString(partition)).get();
        MIRRORMESSAGEAVGLATENCY.labels(taskId, workId, topic, Long.toString(partition)).set(avgLatency);
    }

    public void recordRebalanceNum() {
        REBALANCENUM.labels(taskId, workId).inc();
    }

    public void recordLagSize(String topic, long partition, String consumerGroup, long lag) {
        LAGSIZE.labels(
                taskId,
                workId,
                topic,
                Long.toString(partition),
                consumerGroup
        ).set(lag);
    }
}