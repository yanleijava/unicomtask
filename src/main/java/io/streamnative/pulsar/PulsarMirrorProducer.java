/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.pulsar;

import io.streamnative.common.ConfigUtil;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarMirrorProducer {

    private static final Logger log = (Logger) LoggerFactory.getLogger(PulsarMirrorProducer.class);
    private PulsarClient pulsarClient;
    private Producer pulsarProducer;
    public static final String HEADER_KAFKA_TOPIC_KEY = "__kafka_topic";
    public static final String HEADER_KAFKA_PTN_KEY = "__kafka_partition";
    public static final String HEADER_KAFKA_OFFSET_KEY = "__kafka_offset";

    public PulsarMirrorProducer(PulsarMirrorMakerConfig config, String targetTopic) throws PulsarClientException {

        // create the pulsar client
        try {
            Properties pulsarClientConfig = (Properties) config.pulsarClientConfig.properties.clone();
            Properties pulsarProducerConfig = (Properties) config.pulsarProducerConfig.properties.clone();
            log.info("pulsar client properties {}", pulsarClientConfig);
            log.info("pulsar producer properties {}", pulsarProducerConfig);
            String serviceUrl = pulsarClientConfig.getProperty("brokerServiceUrl");
            pulsarClientConfig.remove("brokerServiceUrl");
            ClientBuilder clientBuilder = PulsarClient.builder();
            pulsarClient = clientBuilder
                    .serviceUrl(serviceUrl)
                    .loadConf(ConfigUtil.convertToMap(pulsarClientConfig))
                    .build();
            ProducerBuilder<byte[]> producerBuilder = this.pulsarClient.newProducer();

            String sendTimeoutMs = pulsarProducerConfig.getProperty("sendTimeoutMs");
            if (StringUtils.isNotBlank(sendTimeoutMs)) {
                producerBuilder.sendTimeout(Integer.valueOf(sendTimeoutMs), TimeUnit.MILLISECONDS);
                pulsarProducerConfig.remove("sendTimeoutMs");
            }
            String batchingMaxPublishDelayMs = pulsarProducerConfig.getProperty("batchingMaxPublishDelayMs");
            if (StringUtils.isNotBlank(batchingMaxPublishDelayMs)) {
                producerBuilder.batchingMaxPublishDelay(Long.valueOf(batchingMaxPublishDelayMs), TimeUnit.MILLISECONDS);
                pulsarProducerConfig.remove("batchingMaxPublishDelayMs");
            }
            producerBuilder.loadConf(ConfigUtil.convertToMap(pulsarProducerConfig));
            pulsarProducer = producerBuilder.topic(targetTopic).create();
        } catch (PulsarClientException e) {
           close();
           throw e;
        }
    }

    protected TypedMessageBuilder newMessage(ConsumerRecord record) {
        TypedMessageBuilder msgBuilder = pulsarProducer.newMessage()
                .property(PulsarMirrorProducer.HEADER_KAFKA_TOPIC_KEY, record.topic())
                .property(PulsarMirrorProducer.HEADER_KAFKA_PTN_KEY, Integer.toString(record.partition()))
                .property(PulsarMirrorProducer.HEADER_KAFKA_OFFSET_KEY, Long.toString(record.offset()));

        if (record.timestamp() > 0) {
            msgBuilder = msgBuilder.eventTime(record.timestamp());
        }
        return msgBuilder;
    }

    CompletableFuture<MessageId> send(ConsumerRecord<Object, Object> record) throws PulsarClientException {
        if (log.isDebugEnabled()) {
            log.debug("key {} value {} topic {} partition {} offset {} string {}",
                    record.key(), record.value(), record.topic(),
                    record.partition(), record.offset(), record.toString());
        }
        if (record.key() != null) {
            return newMessage(record).keyBytes((byte[]) record.key()).value(record.value()).sendAsync();
        } else {
            return newMessage(record).value(record.value()).sendAsync();
        }
    }

    void close() throws PulsarClientException {
        if (pulsarProducer != null) {
            pulsarProducer.close();
        }
        if (pulsarClient != null) {
            pulsarClient.close();
        }
    }
}
