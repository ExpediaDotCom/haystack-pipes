package com.expedia.www.haystack.pipes.commons.kafka.config;

import com.typesafe.config.Config;

import java.util.Map;

public class SpanKeyExtractorConfig {

    private String directory;
    private Map<String, Config> extractorConfig;

    public SpanKeyExtractorConfig(Map<String, Config> extractorConfig) {
        this.directory = "extractors/";
        this.extractorConfig = extractorConfig;
    }

    public String getDirectory() {
        return directory;
    }

    public Map<String, Config> getExtractorConfig() {
        return extractorConfig;
    }
}