/**
 *
 */
package io.streamnative.kop;

import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KopMirrorProducer {

    private static final Logger log = LoggerFactory.getLogger(KopMirrorProducer.class);
    private Producer kopProducer;

    public KopMirrorProducer(KopMirrorMakerConfig config) {
        // create the kop client
        Properties props = config.kopProducerConfig.getProperties();
        log.info("kop producer properties {}", props);
        kopProducer = new KafkaProducer<>(props);
    }

    public void send(ConsumerRecord<Object, Object> record, String topic,
                     KopMirrorThread.PendingMessage pendingMessage) {
        long time = System.currentTimeMillis();
        kopProducer.send(new ProducerRecord<>(topic,
                record.partition(),
                record.timestamp(),
                record.key(),
                record.value()), ((recordMetadata, e) -> {
            if (Objects.isNull(e)) {
                pendingMessage.notWriterNum.decrementAndGet();
            } else {
                synchronized (pendingMessage.exceptions) {
                    if (pendingMessage.exceptions == null) {
                        pendingMessage.exceptions = new HashMap<>();
                    }
                }
                pendingMessage.exceptions.put(topic, e);
                log.error("send topic[{}] error, ", topic, e);
            }
        }));
    }

    void close() {
        if (kopProducer != null) {
            kopProducer.flush();
            kopProducer.close();
        }
    }
}
