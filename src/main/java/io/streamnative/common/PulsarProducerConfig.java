package io.streamnative.common;

import java.io.IOException;
import java.util.Properties;
import lombok.Data;

@Data
public class PulsarProducerConfig {
    public Properties properties;
    public PulsarProducerConfig(String propertiesFile, String propertiesJson)
            throws IOException {
        properties = ConfigUtil.mergeCustomizeConfigToDefaultConfig(propertiesFile, propertiesJson,
                ConfigUtil.DEFAULT_PULSAR_PRODUCER_PROPERTY);
    }
}