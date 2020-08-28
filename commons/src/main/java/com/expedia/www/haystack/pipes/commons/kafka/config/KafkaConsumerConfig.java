package com.expedia.www.haystack.pipes.commons.kafka.config;

import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfig;

public class KafkaConsumerConfig implements KafkaConfig {

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

    @Override
    public String brokers() {
        return this.brokers;
    }

    @Override
    public int port() {
        return this.port;
    }

    @Override
    public String fromtopic() {
        return this.fromTopic;
    }

    @Override
    public String totopic() {
        return this.toTopic;
    }

    @Override
    public int threadcount() {
        return this.threadCount;
    }

    @Override
    public int sessiontimeout() {
        return this.sessionTimeout;
    }

    @Override
    public int maxwakeups() {
        return this.maxWakeUps;
    }

    @Override
    public int wakeuptimeoutms() {
        return this.wakeUpTimeoutMs;
    }

    @Override
    public long polltimeoutms() {
        return this.pollTimeoutMs;
    }

    @Override
    public long commitms() {
        return this.commitMs;
    }

}
