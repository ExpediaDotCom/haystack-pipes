package com.expedia.www.haystack.pipes.commons.kafka.config;

public class KafkaConsumerConfig {

    private String brokers;

    private int port;

    private String fromTopic;

    private String toTopic;

    private int threadCount;

    private int sessionTimeout;

    private int maxWakeUps;

    private int wakeUpTimeoutMs;

    private long pollTimeoutMs;

    private long commitMs;

    public KafkaConsumerConfig(final String brokers, final int port, final String fromTopic, final String toTopic, final int threadCount, final int sessionTimeout, final int maxWakeUps, final int wakeUpTimeoutMs, final long pollTimeoutMs, final long commitMs) {
        this.brokers = brokers;
        this.port = port;
        this.fromTopic = fromTopic;
        this.toTopic = toTopic;
        this.threadCount = threadCount;
        this.sessionTimeout = sessionTimeout;
        this.maxWakeUps = maxWakeUps;
        this.wakeUpTimeoutMs = wakeUpTimeoutMs;
        this.pollTimeoutMs = pollTimeoutMs;
        this.commitMs = commitMs;
    }

    public String getBrokers() {
        return this.brokers;
    }

    public int getPort() {
        return this.port;
    }

    public String getFromTopic() {
        return this.fromTopic;
    }

    public String getToTopic() {
        return this.toTopic;
    }

    public int getThreadCount() {
        return this.threadCount;
    }

    public int getSessionTimeout() {
        return this.sessionTimeout;
    }

    public int getMaxWakeUps() {
        return this.maxWakeUps;
    }

    public int getWakeUpTimeoutMs() {
        return this.wakeUpTimeoutMs;
    }

    public long getPollTimeoutMs() {
        return this.pollTimeoutMs;
    }

    public long getCommitMs() {
        return this.commitMs;
    }
}
