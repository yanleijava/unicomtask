/**
 *
 */
package io.streamnative.pulsar;

import io.streamnative.common.ConfigUtil;
import io.streamnative.common.KafkaConsumerConfig;
import io.streamnative.common.PulsarClientConfig;
import io.streamnative.common.PulsarProducerConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class PulsarMirrorMakerConfig {
    private static final Logger log = LoggerFactory.getLogger(PulsarMirrorMakerConfig.class);

    public int listenPort;
    public String taskId;
    public String workId;
    public int numKafkaConsumerInstances;
    public String kafkaSourceTopics;
    public Map<String, Set<String>> mirrorTopicMapping = new HashMap<>();
    public KafkaConsumerConfig kafkaConsumerConfig;
    public PulsarClientConfig pulsarClientConfig;
    public PulsarProducerConfig pulsarProducerConfig;
    public String jvmGCMetricsLoggerClassName = "";

    public void parseMappingList(String mappingList) throws JsonProcessingException {
        kafkaSourceTopics = ConfigUtil.parseMappingList(mappingList, mirrorTopicMapping, kafkaSourceTopics);
    }

    public Set<String> getAllTargetTopics() {
        Set<String> topics = new HashSet<>();
        mirrorTopicMapping.values().forEach(topicSet -> {
            topics.addAll(topicSet);
        });
        return topics;
    }
}
