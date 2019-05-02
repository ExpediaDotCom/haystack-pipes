/*
 * Copyright 2018 Expedia, Inc.
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

package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.serialization.SpanProtobufDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerTask implements Runnable, Closeable{
    private static Logger logger = LoggerFactory.getLogger(ConsumerTask.class);
    private final ScheduledExecutorService wakeupScheduler;
    private final KafkaConfig config;
    private final HealthController healthController;
    private int wakeups = 0;
    private final KafkaConsumer<String, Span> consumer;
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final Map<Integer, SpanProcessor> processors = new HashMap<>();
    private Instant lastCommitTimestamp = Instant.now();

    public ConsumerTask(final KafkaConfig config,
                        final Class<?> containingClass,
                        final SpanProcessorSupplier processorSupplier,
                        final HealthController healthController) {
        this.config = config;
        this.consumer = new KafkaConsumer<>(getProperties(containingClass), new StringDeserializer(), new SpanProtobufDeserializer("haystack-pipes-firehose-writer"));
        this.wakeupScheduler = Executors.newScheduledThreadPool(1);
        this.healthController = healthController;

        consumer.subscribe(Collections.singletonList(config.fromtopic()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
                synchronized (this) {
                    topicPartitions.forEach(topicPartition -> {
                        final SpanProcessor processor = processors.remove(topicPartition.partition());
                        if (processor != null) {
                            processor.close();
                        }
                    });
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
                synchronized (this) {
                    topicPartitions.forEach(topicPartition -> {
                        SpanProcessor processor = processors.get(topicPartition.partition());
                        if(processor == null) {
                            processor = processorSupplier.get();
                            processors.put(topicPartition.partition(), processor);
                            processor.init(topicPartition);
                        }
                    });
                }
            }
        });
    }

    @Override
    public void run() {
        while (!shutdownRequested.get()) {
            final ConsumerRecords<String, Span> records = poll(consumer);
            if (records != null && !records.isEmpty()) {
                final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                records.forEach(record -> {
                    final int partition = record.partition();
                    final SpanProcessor processor = processors.get(partition);
                    if (processor != null) {
                        final Optional<Long> commitableOffset = processor.process(record);
                        if (commitableOffset.isPresent()) {
                            offsets.putIfAbsent(new TopicPartition(config.fromtopic(), partition),
                                    new OffsetAndMetadata(commitableOffset.get()));
                        }
                    } else {
                        logger.error("Unexpected !! Fail to find the partition for the incoming span record");
                    }
                });

                if (!offsets.isEmpty() && Instant.now().minusMillis(config.commitms()).isAfter(lastCommitTimestamp)) {
                    lastCommitTimestamp = Instant.now();
                    commitSync(offsets);
                }
            }
        }
    }

    private void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            logger.info("committing offset now {}", offsets);
            consumer.commitSync(offsets);
        } catch (Exception ex) {
            logger.error("Fail to commit offsets with error", ex);
        }
    }

    private ConsumerRecords<String, Span> poll(final KafkaConsumer<String, Span> consumer) {
        final ScheduledFuture<?> wakeupCall = wakeupScheduler.schedule(consumer::wakeup,
                this.config.wakeuptimeoutms(),
                TimeUnit.MILLISECONDS);
        try {
            final ConsumerRecords<String, Span> records = consumer.poll(this.config.polltimeoutms());
            wakeups = 0;
            return records;
        } catch (WakeupException we) {
            if (shutdownRequested.get()) {
                throw we;
            }
            wakeups = wakeups + 1;
            if (wakeups >= this.config.maxwakeups()) {
                logger.error("WakeupException limit exceeded, throwing up wakeup exception", we);
                healthController.setUnhealthy();
                throw we;
            } else {
                logger.error("Consumer poll took more than {} ms for consumer, wakeup attempt={}!. Will try poll again!",
                        this.config.wakeuptimeoutms(), wakeups);
            }
        } finally {
            wakeupCall.cancel(true);
        }
        return null;
    }

    private Properties getProperties(Class<?> containingClass) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, containingClass.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.config.brokers() + ":" + this.config.port());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.config.sessiontimeout());
        return props;
    }

    @Override
    public void close() throws IOException {
        shutdownRequested.set(true);
        consumer.wakeup();
        consumer.close(10, TimeUnit.SECONDS);
        wakeupScheduler.shutdown();
    }
}
