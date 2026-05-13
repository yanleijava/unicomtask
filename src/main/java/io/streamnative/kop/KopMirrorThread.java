/**
 *
 */
package io.streamnative.kop;

import io.streamnative.pulsar.KafkaMirrorConsumer;
import io.streamnative.pulsar.PulsarMirrorMakerStat;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KopMirrorThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(KopMirrorThread.class);

    private final AtomicBoolean isClosed;
    private final KafkaMirrorConsumer consumer;
    private final Map<String, KopMirrorProducer> producerMap;
    private final KopMirrorMakerConfig config;
    private final PulsarMirrorMakerStat stat;
    private final Map<String, LinkedBlockingQueue<PendingMessage>> pendingMessagesMap = new ConcurrentHashMap<>();
    private Long lastLagTimeStamp = System.currentTimeMillis();
    public Map<TopicPartition, Long> lagMap = new ConcurrentHashMap<>();

    public KopMirrorThread(KafkaMirrorConsumer consumer, Map<String, KopMirrorProducer> producerMap,
                           AtomicBoolean isClosed, KopMirrorMakerConfig config, PulsarMirrorMakerStat stat) {
        this.consumer = consumer;
        this.producerMap = producerMap;
        this.isClosed = isClosed;
        this.config = config;
        this.stat = stat;
        consumer.subscribe(config.kafkaSourceTopics);
    }

    public KafkaMirrorConsumer getConsumer() {
        return consumer;
    }

    public static class PendingMessage {
        final String topic;
        final int partition;
        final long offset;
        final long msgSize;
        final long startTimeMs;
        AtomicInteger notWriterNum;
        Map<String, Exception> exceptions;
        private PendingMessage(
                ConsumerRecord<Object, Object> record) {
            this.topic = record.topic();
            this.partition = record.partition();
            this.offset = record.offset();
            this.startTimeMs = System.currentTimeMillis();
            long msgSize = 0;
            if (record.serializedValueSize() > 0 || record.serializedKeySize() > 0) {
                if (record.serializedKeySize() > 0) {
                    msgSize += record.serializedKeySize();
                }
                if (record.serializedValueSize() > 0) {
                    msgSize += record.serializedValueSize();
                }
            }
            this.msgSize = msgSize;
        }

        private boolean isDone() {
            if (notWriterNum.get() > 0) {
                return false;
            } else {
                return true;
            }
        }

        private boolean hasError() {
            if (exceptions == null) {
                return false;
            } else {
                return true;
            }
        }
    }


    private void sendAll(ConsumerRecord<Object, Object> record,
                                  KopMirrorThread.PendingMessage pendingMessage) throws IllegalArgumentException {
        String srcTopic = record.topic();
        Set<String> targetTopicSet = config.mirrorTopicMapping.get(srcTopic);
        pendingMessage.notWriterNum = new AtomicInteger(targetTopicSet.size());
        for (String topic : targetTopicSet) {
            KopMirrorProducer producer = producerMap.get(topic);
            if (producer == null) {
                throw new IllegalArgumentException("producer for " + topic + " is not init");
            }
            producer.send(record, topic, pendingMessage);
        }
    }

    private void consumeKafkaRecordsLoop() throws InterruptedException, ExecutionException {
        long lastPollTimeStamp = System.currentTimeMillis();
        ConsumerRecords<Object, Object> consumerRecords;
        long getPartitionLagTimeStamp = System.currentTimeMillis();
        if (System.currentTimeMillis() - lastLagTimeStamp > 1000 * 30) {
            lagMap = consumer.getPartitionLag();
            lastLagTimeStamp = System.currentTimeMillis();
        }
        long getPartitionLagUseTime = System.currentTimeMillis() - getPartitionLagTimeStamp;
        long pollTimeStamp = System.currentTimeMillis();
        consumerRecords = consumer.getConsumer().poll(Duration.ofMillis(1000));
        long pollUseTime = System.currentTimeMillis() - pollTimeStamp;
        Iterator<ConsumerRecord<Object, Object>> recordIterator = consumerRecords.iterator();
        int pollNumMsg = consumerRecords.count();
        long sendAsyncTimeStamp = System.currentTimeMillis();
        while (recordIterator.hasNext()) {
            ConsumerRecord<Object, Object> record = recordIterator.next();
            LinkedBlockingQueue<KopMirrorThread.PendingMessage> pendingMessageList =
                    pendingMessagesMap.get(record.topic());
            if (pendingMessageList == null) {
                pendingMessageList = new LinkedBlockingQueue<KopMirrorThread.PendingMessage>();
                pendingMessagesMap.put(record.topic(), pendingMessageList);
            }
            KopMirrorThread.PendingMessage pendingMessage = new KopMirrorThread.PendingMessage(record);
            sendAll(record, pendingMessage);
            pendingMessageList.put(pendingMessage);
            if (log.isDebugEnabled()) {
                log.debug("topic {} put pendingMessageList {}", record.topic(), pendingMessageList.size());
            }
        }
        long sendAsyncUseTime = System.currentTimeMillis() - sendAsyncTimeStamp;
        Iterator<Map.Entry<String, LinkedBlockingQueue<KopMirrorThread.PendingMessage>>> iterator =
                pendingMessagesMap.entrySet().iterator();
        long commitAsyncTimeStamp = System.currentTimeMillis();
        while (iterator.hasNext()) {
            Map.Entry<String, LinkedBlockingQueue<KopMirrorThread.PendingMessage>> entry = iterator.next();
            String topic = entry.getKey();
            LinkedBlockingQueue<KopMirrorThread.PendingMessage> pendingMessageList = entry.getValue();
            KopMirrorThread.PendingMessage msg = pendingMessageList.peek();
            Map<TopicPartition, Long> offsets = new HashMap<>();
            Map<String, KopMirrorThread.MetricsEntry> statCache = new HashMap<>();
            long totalMsgSize = 0;
            long totalMsgNum = 0;
            int totalSendUseTime = 0;
            while (msg != null && msg.isDone()) {
                if (msg.hasError()) {
                    break;
                }
                String topicPartitionKey = msg.topic + "#" + msg.partition;
                long msgSize = msg.msgSize;
                totalMsgNum += 1;
                if (msgSize > 0) {
                    totalMsgSize += msgSize;
                }
                long senduseTime = System.currentTimeMillis() - msg.startTimeMs;
                totalSendUseTime += senduseTime;
                long offset = msg.offset + 1;
                addMetrics(statCache, topicPartitionKey, 1, msgSize, senduseTime, offset);
                pendingMessageList.poll();
                msg = pendingMessageList.peek();
            }
            stat.recordMessageNum(totalMsgNum);
            stat.recordBytes(totalMsgSize);
            stat.recordProcessLatency(totalSendUseTime);
            stat.recordPendingMessageListStatus(topic, consumer.getConsumer().groupMetadata().groupId(), pollNumMsg,
                    totalMsgNum, pendingMessageList.size());

            for (Map.Entry<String, KopMirrorThread.MetricsEntry> e : statCache.entrySet()) {
                String[] array = e.getKey().split("#");
                TopicPartition topicPartition = new TopicPartition(array[0], Integer.valueOf(array[1]));
                KopMirrorThread.MetricsEntry metricsEntry = e.getValue();
                stat.recordMessageTopicNum(topicPartition, metricsEntry.msgNum);
                stat.recordTopicBytes(topicPartition, metricsEntry.msgSize);
                stat.recordTopicProcessLatency(topicPartition, metricsEntry.senduseTime);
                offsets.put(topicPartition, metricsEntry.offset);
            }
            if (!offsets.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
                Iterator<Map.Entry<TopicPartition, Long>> it = offsets.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<TopicPartition, Long> entryOffset = it.next();
                    TopicPartition topicPartition = entryOffset.getKey();
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(entryOffset.getValue());
                    consumer.rebalanceListner.addOffset(topicPartition, offsetAndMetadata);
                    offsetAndMetadataMap.put(topicPartition, offsetAndMetadata);
                }
                consumer.getConsumer().commitAsync(offsetAndMetadataMap, (offsetsMap, exception) -> {
                    if (null != exception) {
                        // we can still continue because the data can still be written to pulsar
                        log.warn("Failed to commit Kafka offsets {}", offsets, exception);
                    } else {
                        log.info("Commit Kafka offset {} succeed", offsets);
                    }
                });
            }
        }
        long commitAsyncUseTime = System.currentTimeMillis() - commitAsyncTimeStamp;
        if (pollNumMsg > 0) {
            log.info("consumer({}) polled {} message. allUseTime {}, getPartitionLagUseTime {}, pollUseTime {},"
                            + " sendAsyncUseTime {}, commitAsyncUseTime {} .",
                    consumer.getConsumer().groupMetadata().memberId(),
                    pollNumMsg,
                    (System.currentTimeMillis() - lastPollTimeStamp),
                    getPartitionLagUseTime,
                    pollUseTime,
                    sendAsyncUseTime,
                    commitAsyncUseTime
            );
        }
    }

    private void addMetrics(Map<String, MetricsEntry> metricsMap, String topicPartition, long msgNum, long msgSize,
                            long senduseTime, long offset) {
        MetricsEntry metricsEntry = metricsMap.get(topicPartition);
        if (metricsEntry == null) {
            metricsEntry = new MetricsEntry();
            metricsEntry.msgNum = msgNum;
            metricsEntry.senduseTime = senduseTime;
            metricsEntry.msgSize = msgSize;
            metricsEntry.offset = offset;
            metricsMap.put(topicPartition, metricsEntry);
        } else {
            metricsEntry.msgNum += msgNum;
            metricsEntry.senduseTime += senduseTime;
            metricsEntry.msgSize += msgSize;
            metricsEntry.offset = offset;
        }
    }

    public void run() {
        while (!isClosed.get()) {
            if (isInterrupted()) {
                log.info("pulsar mirror maker is interrupted");
                close();
            }
            try {
                consumeKafkaRecordsLoop();
            }  catch (InterruptedException | ExecutionException e) {
                log.error("kop mirror maker process failed, ", e);
                close();
            } catch (KafkaException e) {
                if (e.getCause() instanceof CommitFailedException) {
                    //avoid thread stop
                    e.printStackTrace();
                    log.warn("kafka commit failed, ", e);
                } else {
                    e.printStackTrace();
                    log.warn("Kafka failed, ", e);
                }
            } catch (Throwable e) {
                e.printStackTrace();
                log.error("process failed, ", e);
                close();
            }
        }
    }

    public void close() {
        Iterator<Map.Entry<String, KopMirrorProducer>> iterator = producerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, KopMirrorProducer> entry = iterator.next();
            try {
                entry.getValue().close();
                log.info("close kop producer for topics {}", entry.getKey());
            } catch (Exception e) {
                log.error("close kop producer for topics {},", entry.getKey());
            }
        }
        consumer.close();
        isClosed.set(true);
    }

    private class MetricsEntry {
        long msgNum;
        long msgSize;
        long senduseTime;
        long offset;
    }
}
