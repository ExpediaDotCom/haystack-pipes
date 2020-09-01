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
package com.expedia.www.haystack.pipes.kafkaProducer;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.expedia.www.haystack.commons.metrics.MetricsRegistries;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.expedia.www.haystack.pipes.kafkaProducer.key.extractor.JsonExtractor;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.key.extractor.loader.SpanKeyExtractorLoader;
import com.netflix.servo.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.expedia.www.haystack.pipes.kafkaProducer.Constants.APPLICATION;


public class Service {

    private static final MetricRegistry metricRegistry = MetricsRegistries.metricRegistry();
    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(Service.class);
    @VisibleForTesting
    static KafkaStreamStarter kafkaStreamStarter;
    @VisibleForTesting
    static ProtobufToKafkaProducer protobufToKafkaProducer;
    private static SerdeFactory serdeFactory;
    private static ProjectConfiguration projectConfiguration;
    private static KafkaToKafkaPipeline kafkaToKafkaPipeline;
    private static HealthController healthController;

    public static void main(String[] args) {
        logger.info("Starting KafkaToKafka Producer Pipeline");
        initialize();
        startJmxReporter();
        startService();
    }

    private static void initialize() {
        serdeFactory = new SerdeFactory();
        projectConfiguration = ProjectConfiguration.getInstance();
        kafkaToKafkaPipeline = prepareKafkaToKafkaPipeline();
        healthController = getHealthController();
    }


    private static void startService() {
        kafkaStreamStarter = new KafkaStreamStarter(ProtobufToKafkaProducer.class, APPLICATION, projectConfiguration.getKafkaConsumerConfig(), healthController);
        protobufToKafkaProducer = new ProtobufToKafkaProducer(
                kafkaStreamStarter, serdeFactory, kafkaToKafkaPipeline, projectConfiguration.getKafkaConsumerConfig());
        protobufToKafkaProducer.main();
    }

    private static HealthController getHealthController() {
        final HealthController healthController = new HealthController();
        healthController.addListener(new UpdateHealthStatusFile("/app/isHealthy"));
        return healthController;
    }


    private static KafkaToKafkaPipeline prepareKafkaToKafkaPipeline() {
        List<SpanKeyExtractor> spanKeyExtractors = SpanKeyExtractorLoader.getInstance(projectConfiguration.getSpanExtractorConfigs()).getSpanKeyExtractor();
        spanKeyExtractors.add(new JsonExtractor());// adding default extractor for backward compatibility
        return new KafkaToKafkaPipeline(metricRegistry, projectConfiguration, spanKeyExtractors);
    }

    private static void startJmxReporter() {
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        jmxReporter.start();
    }

}
