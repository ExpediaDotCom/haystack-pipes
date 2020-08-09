package com.expedia.www.haystack.pipes.commons.key.extractor;

import com.expedia.www.haystack.commons.config.ConfigurationLoader;
import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectConfiguration {

    private final Config haystackConf;
    private final String directory;

    public ProjectConfiguration() {
        Config config = ConfigurationLoader.loadConfigFileWithEnvOverrides("config/extractors.conf", "HAYSTACK_PROP_");
        haystackConf = config.getConfig("haystack");
        directory = haystackConf.getString("directory");
    }

    public Map<String, Config> getSpanExtractorConfigs() {
        Map<String, Config> extractorConfigMap = new HashMap<>();
        if (haystackConf != null) {
            List<Config> extractorConfigs = (List<Config>) haystackConf.getConfigList("extractors");
            extractorConfigs.forEach(extractorConfig -> {
                String name = extractorConfig.getString("name");
                Config config = extractorConfig.getConfig("config");
                extractorConfigMap.put(name, config);
            });
        }
        return extractorConfigMap;
    }

    public String getDirectory() {
        return directory;
    }
}
