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
package com.expedia.www.haystack.pipes.producer;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.expedia.www.haystack.commons.metrics.MetricsRegistries;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.key.extractor.loader.SpanKeyExtractorLoader;
import com.expedia.www.haystack.pipes.producer.key.extractor.JsonExtractor;
import com.netflix.servo.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.expedia.www.haystack.pipes.producer.Constants.APPLICATION;


public class Service {

    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(Service.class);
    private static final MetricRegistry metricRegistry = MetricsRegistries.metricRegistry();

    public static void main(String[] args) {
        logger.info("Starting KafkaToKafka Producer Pipeline");
        startService();
    }

    private static void startService() {
        SerdeFactory serdeFactory = new SerdeFactory();
        ProjectConfiguration projectConfiguration = ProjectConfiguration.getInstance();
        KafkaToKafkaPipeline kafkaToKafkaPipeline = prepareKafkaToKafkaPipeline(projectConfiguration);
        final HealthController healthController = getHealthController();
        startJmxReporter();
        KafkaStreamStarter kafkaStreamStarter = new KafkaStreamStarter(ProtobufToKafkaProducer.class, APPLICATION, projectConfiguration.getKafkaConsumerConfig(), healthController);
        ProtobufToKafkaProducer protobufToKafkaProducer = new ProtobufToKafkaProducer(
                kafkaStreamStarter, serdeFactory, kafkaToKafkaPipeline, projectConfiguration.getKafkaConsumerConfig());
        protobufToKafkaProducer.main();
    }

    private static HealthController getHealthController() {
        final HealthController healthController = new HealthController();
        healthController.addListener(new UpdateHealthStatusFile("/app/isHealthy"));
        return healthController;
    }


    private static KafkaToKafkaPipeline prepareKafkaToKafkaPipeline(ProjectConfiguration projectConfiguration) {
        List<SpanKeyExtractor> spanKeyExtractors = SpanKeyExtractorLoader.getInstance(projectConfiguration.getSpanExtractorConfigs()).getSpanKeyExtractor();
        spanKeyExtractors.add(new JsonExtractor());// adding default extractor for backward compatibility
        return new KafkaToKafkaPipeline(metricRegistry, projectConfiguration, spanKeyExtractors);
    }

    private static void startJmxReporter() {
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        jmxReporter.start();
    }

}
