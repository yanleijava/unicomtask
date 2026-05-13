package io.streamnative.common;

import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarClientConfig {
    private static final Logger log = LoggerFactory.getLogger(PulsarClientConfig.class);

    public Properties properties;
    public PulsarClientConfig(String propertiesFile, String propertiesJson) throws IOException {
        properties = ConfigUtil.mergeCustomizeConfigToDefaultConfig(propertiesFile, propertiesJson,
                ConfigUtil.DEFAULT_PULSAR_CLIENT_PROPERTY);
        if (!properties.containsKey("brokerServiceUrl")) {
            log.error("pulsar brokerServiceUrl must set value!");
            System.exit(0);
        }
    }
}