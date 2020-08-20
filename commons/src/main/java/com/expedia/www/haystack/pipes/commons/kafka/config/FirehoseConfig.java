package com.expedia.www.haystack.pipes.commons.kafka.config;

public class FirehoseConfig {

    private String url;

    private String streamName;

    private String signingRegion;

    private int initialRetrySleep;

    private int maxRetrySleep;

    private boolean useStringBuffering;

    private int maxBatchInterval;

    private int maxParallelISMPerShard;

    public FirehoseConfig(final String url, final String streamName, final String signingRegion, final int initialRetrySleep, final int maxRetrySleep, final boolean useStringBuffering, final int maxBatchInterval, final int maxParallelISMPerShard) {
        this.url = url;
        this.streamName = streamName;
        this.signingRegion = signingRegion;
        this.initialRetrySleep = initialRetrySleep;
        this.maxRetrySleep = maxRetrySleep;
        this.useStringBuffering = useStringBuffering;
        this.maxBatchInterval = maxBatchInterval;
        this.maxParallelISMPerShard = maxParallelISMPerShard;
    }

    public String getUrl() {
        return this.url;
    }

    public String getStreamName() {
        return this.streamName;
    }

    public String getSigningRegion() {
        return this.signingRegion;
    }

    public int getInitialRetrySleep() {
        return this.initialRetrySleep;
    }

    public int getMaxRetrySleep() {
        return this.maxRetrySleep;
    }

    public boolean isUseStringBuffering() {
        return this.useStringBuffering;
    }

    public int getMaxBatchInterval() {
        return this.maxBatchInterval;
    }

    public int getMaxParallelISMPerShard() {
        return this.maxParallelISMPerShard;
    }
}
