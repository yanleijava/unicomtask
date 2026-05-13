package io.streamnative.common;

import java.io.IOException;
import java.util.Properties;
import lombok.Data;

@Data
public class KopProducerConfig {

    public Properties properties;
    public KopProducerConfig(String propertiesFile, String propertiesJson)
            throws IOException {
        properties = ConfigUtil.mergeCustomizeConfigToDefaultConfig(propertiesFile, propertiesJson,
                ConfigUtil.DEFAULT_KOP_PRODUCER_PROPERTY);
    }
}
