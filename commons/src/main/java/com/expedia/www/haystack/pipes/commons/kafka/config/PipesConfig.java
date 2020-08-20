package com.expedia.www.haystack.pipes.commons.kafka.config;

import com.expedia.www.haystack.metrics.GraphiteConfig;

public class PipesConfig {

    private int replicationFactor;

    public PipesConfig(final int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public int getReplicationFactor() {
        return this.replicationFactor;
    }

}
