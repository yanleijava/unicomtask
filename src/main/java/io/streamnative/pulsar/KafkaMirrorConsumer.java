/**
 *
 */
package io.streamnative.pulsar;

import io.streamnative.common.KafkaConsumerConfig;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMirrorConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaMirrorConsumer.class);
    public KafkaConsumerConfig config;
    private PulsarMirrorMakerStat stat;
    private Consumer consumer;
    public RebalanceListner rebalanceListner;

    public static class RebalanceListner implements ConsumerRebalanceListener {
        private static final Logger rlog = LoggerFactory.getLogger(RebalanceListner.class);

        private Consumer consumer;
        private PulsarMirrorMakerStat stat;
        public Set<TopicPartition> topicPartitions = new HashSet<>();
        private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();

        public RebalanceListner(Consumer consumer, PulsarMirrorMakerStat stat) {
            this.consumer = consumer;
            this.stat = stat;
        }

        public void addOffset(TopicPartition topicPartition,  OffsetAndMetadata offsetAndMetadata) {
            currentOffsets.put(topicPartition, offsetAndMetadata);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            this.topicPartitions.clear();
            this.topicPartitions.addAll(partitions);
            stat.recordRebalanceNum();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("Following Partitions Assigned: ");
            Iterator<TopicPartition> iterator = partitions.iterator();
            while (iterator.hasNext()) {
                TopicPartition tp = iterator.next();
                stringBuilder.append("[").append(tp.topic()).append(",").append(tp.partition())
                        .append("]");
                if (iterator.hasNext()) {
                    stringBuilder.append(";");
                }
            }
            rlog.info(stringBuilder.toString());
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("Following Partitions Revoked:");
            Iterator<TopicPartition> iterator = partitions.iterator();
            while (iterator.hasNext()) {
                TopicPartition tp = iterator.next();
                stringBuilder.append("[").append(tp.topic()).append(",").append(tp.partition())
                        .append("]");
                if (iterator.hasNext()) {
                    stringBuilder.append(";");
                }
            }
            rlog.info(stringBuilder.toString());
            stringBuilder = new StringBuilder();
            stringBuilder.append("Following Partitions commited:");
            Iterator<Map.Entry<TopicPartition, OffsetAndMetadata>> iterator2 = currentOffsets.entrySet().iterator();
            while (iterator2.hasNext()) {
                Map.Entry<TopicPartition, OffsetAndMetadata> entry = iterator2.next();
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = entry.getValue();
                stringBuilder.append("[").append(tp.toString()).append(",").append(offsetAndMetadata.offset())
                        .append("]");
                if (iterator2.hasNext()) {
                    stringBuilder.append(";");
                }
            }
            rlog.info("Following Partitions commited:" + stringBuilder.toString());
            try {
                consumer.commitSync(currentOffsets);
                rlog.info("Followed Partitions commited:" + stringBuilder.toString());
                currentOffsets.clear();
            } catch (Exception e) {
                rlog.error("Partitions[{}] commited failed,", currentOffsets, e);
                currentOffsets.clear();
            }
        }
    }

    public KafkaMirrorConsumer(KafkaConsumerConfig config, PulsarMirrorMakerStat stat) {
        this.config = config;
        this.stat = stat;
        initializeKafkaMirrorConsumer();
        this.rebalanceListner = new RebalanceListner(this.consumer, stat);
    }

    public void subscribe(String topics) {
        String regex = topics.trim().replace(',', '|')
                .replace(" ", "")
                .replaceAll("^[\"']+", "")
                .replaceAll("[\"']+$", "");
        log.info("subscribe kafka topics {} regex {}", topics, regex);
        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            log.error("regex syntx exception for {}", regex, e);
            throw e;
        }
        consumer.subscribe(Pattern.compile(regex), rebalanceListner);
    }

    private void initializeKafkaMirrorConsumer() {
        Properties props = config.getProperties();
        log.info("Kafka consumer properties {}", props);
        consumer = new KafkaConsumer(props);
    }

    public Map<TopicPartition, Long> getPartitionLag() {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(this.rebalanceListner.topicPartitions);
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(this.rebalanceListner.topicPartitions);
        Map<TopicPartition, OffsetAndMetadata> commitoffsets =
                consumer.committed(this.rebalanceListner.topicPartitions);
        Map<TopicPartition, Long> lagMap = new ConcurrentHashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            if (entry.getValue().longValue() > 0 && commitoffsets.get(entry.getKey()) != null) {
                long endOffset = entry.getValue().longValue();
                long commitedOffset = commitoffsets.get(entry.getKey()).offset();
                lagMap.put(entry.getKey(), endOffset - commitedOffset);
            } else {
                long endOffset = entry.getValue().longValue();
                long beginOffset = beginningOffsets.getOrDefault(entry.getKey(), 0L);
                log.warn("{} has no commited offset, will regard begin offset as last committed offset, "
                        + "beginOffset {} endOffset {}", entry.getKey(), beginOffset, endOffset);
                lagMap.put(entry.getKey(), endOffset - beginOffset);
            }
        }
        return lagMap;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void close() {
        consumer.close();
    }
}
