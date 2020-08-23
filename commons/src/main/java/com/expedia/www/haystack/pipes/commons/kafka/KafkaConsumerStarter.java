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

import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.commons.kafka.config.ProjectConfiguration;
import com.netflix.servo.util.VisibleForTesting;
import org.cfg4j.provider.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KafkaConsumerStarter {
    static final String STARTED_MSG = "Now started Stream %s";
    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(KafkaConsumerStarter.class);
    @VisibleForTesting
    static ConfigurationProvider CONFIGURATION_PROVIDER = new com.expedia.www.haystack.commons.config.Configuration().createMergeConfigurationProvider();
    public final Class<?> containingClass;
    public final String clientId;
    private final HealthController healthController;
    private final List<ConsumerTask> tasks;

    public KafkaConsumerStarter(Class<?> containingClass,
                                String clientId,
                                HealthController healthController) {
        this.containingClass = containingClass;
        this.clientId = clientId;
        this.healthController = healthController;
        this.tasks = new ArrayList<>();
    }

    private static KafkaConsumerConfig getKafkaConfig() {
        return ProjectConfiguration.getInstance().getKafkaConsumerConfig();
    }

    public void createAndStartConsumer(SpanProcessorSupplier processorSupplier) {
        for (int idx = 0; idx <= getThreadCount(); idx++) {
            final ConsumerTask task = new ConsumerTask(getKafkaConfig(), containingClass, processorSupplier, healthController);
            this.tasks.add(task);
            final Thread thread = new Thread(task);
            thread.setDaemon(true);
            thread.start();
        }
        healthController.setHealthy();
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        logger.info(String.format(STARTED_MSG, containingClass.getSimpleName()));
    }

    private void close() {
        tasks.forEach(task -> {
            try {
                task.close();
            } catch (IOException ignored) {
            }
        });
    }

    private int getThreadCount() {
        final KafkaConsumerConfig kafkaConfig = getKafkaConfig();
        return kafkaConfig.getThreadCount();
    }
}
