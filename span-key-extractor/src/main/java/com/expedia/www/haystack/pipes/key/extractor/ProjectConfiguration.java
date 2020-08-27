/*
 * Copyright 2020 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.key.extractor;

import com.expedia.www.haystack.commons.config.ConfigurationLoader;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ProjectConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ProjectConfiguration.class);

    private final Config haystackConf;
    private final String directory;

    public ProjectConfiguration() {
        Config config = ConfigurationLoader.loadConfigFileWithEnvOverrides("config/base.conf", Optional.empty().toString());
        logger.debug("loaded config: {}", config);
        haystackConf = config.getConfig(Constants.HAYSTACK_KEY);
        directory = haystackConf.getString(Constants.DIRECTORY_KEY);

    }

    public Map<String, Config> getSpanExtractorConfigs() {
        Map<String, Config> extractorConfigMap = new HashMap<>();
        if (haystackConf != null) {
            List<Config> extractorConfigs = (List<Config>) haystackConf.getConfigList(Constants.EXTRACTORS_KEY);
            extractorConfigs.forEach(extractorConfig -> {
                String name = extractorConfig.getString(Constants.EXTRACTOR_NAME_KEY);
                Config config = extractorConfig.getConfig(Constants.EXTRACTOR_CONFIG_KEY);
                extractorConfigMap.put(name, config);
            });
        }
        return extractorConfigMap;
    }

    public String getDirectory() {
        return directory;
    }
}
