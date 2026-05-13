package io.streamnative.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;

public class ConfigUtil {
    //    public static final String DEFAULT_KAFAK_CONSUMER_PROPERTY = "../conf/kafkaConsumer.properties";
    public static final String DEFAULT_KAFAK_CONSUMER_PROPERTY =
            "/Users/liudezhi/git/new/kafkaPulsarMirrorMaker/conf/kafkaConsumer.properties";

    //    public static final String DEFAULT_KOP_PRODUCER_PROPERTY = "../conf/kopProducer.conf";
    public static final String DEFAULT_KOP_PRODUCER_PROPERTY =
            "/Users/liudezhi/git/new/kafkaPulsarMirrorMaker/conf/kopProducer.properties";

    public static final String DEFAULT_PULSAR_CLIENT_PROPERTY = "../conf/pulsarClient.conf";
    public static final String DEFAULT_PULSAR_PRODUCER_PROPERTY = "../conf/pulsarProducer.conf";

    public static final String COMPRESSION_NONE = "NONE";
    public static final String COMPRESSION_SNAPPY = "SNAPPY";
    public static final String COMPRESSION_ZLIB = "ZLIB";
    public static final String COMPRESSION_ZSTD = "ZSTD";
    public static final String COMPRESSION_LZ4 = "LZ4";

    /*
     * 根据自定义参数和默认的文件参数进行合并
     */
    public static Properties mergeCustomizeConfigToDefaultConfig(String customizeConfig, String propertiesJson,
                                                                  String defaultConfig)
            throws IOException {
        Properties customizeProperties = null;
        Properties defaultProperties = null;
        if (!StringUtils.isEmpty(customizeConfig)) {
            InputStream inputStream = new FileInputStream(customizeConfig);
            customizeProperties = new Properties();
            customizeProperties.load(inputStream);
            inputStream.close();
        }
        if (!StringUtils.isEmpty(propertiesJson)) {
            ObjectMapper objectMapper = new ObjectMapper();
            customizeProperties = objectMapper.readValue(propertiesJson, Properties.class);
        }
        if (!StringUtils.isEmpty(defaultConfig)) {
            InputStream inputStream = new FileInputStream(defaultConfig);
            defaultProperties = new Properties();
            defaultProperties.load(inputStream);
            inputStream.close();
        }
        if (customizeProperties != null) {
            defaultProperties.putAll(customizeProperties);
        }
        return defaultProperties;
    }

    public static Map<String, Object> convertToMap(Properties properties) {
        Map<String, Object> map = new HashMap<String, Object>((Map) properties);
        return map;
    }

    /**
     * 解析原端Topic到目的端映射关系.
     * @param mappingList 配置传入的JSON的映射字符串
     * @param mirrorTopicMapping 解析后存入的映射对象
     * @param kafkaSourceTopics 原端Topic列表
     * @return kafkaSourceTopics
     */
    public static String parseMappingList(String mappingList, Map<String, Set<String>> mirrorTopicMapping,
                                 String kafkaSourceTopics) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        MappingTopicsList mappingTopicsList = objectMapper.readValue(mappingList, MappingTopicsList.class);
        for (int i = 0; i < mappingTopicsList.getTopics().size(); i++) {
            MappingTopics mapping = mappingTopicsList.getTopics().get(i);
            for (int j = 0; j < mapping.inputTopics.size(); j++) {
                String inputTopic = mapping.inputTopics.get(j);
                String outTopic = mapping.outputTopic;
                Set<String> outTopicSet = mirrorTopicMapping.get(inputTopic);
                if (outTopicSet == null) {
                    outTopicSet = new HashSet<>();
                }
                outTopicSet.add(outTopic);
                mirrorTopicMapping.put(inputTopic, outTopicSet);
            }
        }
        for (String key : mirrorTopicMapping.keySet()) {
            if (kafkaSourceTopics == null || kafkaSourceTopics.equals("")) {
                kafkaSourceTopics = key;
            } else {
                kafkaSourceTopics = kafkaSourceTopics + "," + key;
            }
        }
        return kafkaSourceTopics;
    }
}
