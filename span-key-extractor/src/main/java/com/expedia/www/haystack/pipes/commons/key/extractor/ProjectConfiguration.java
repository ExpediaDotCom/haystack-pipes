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
