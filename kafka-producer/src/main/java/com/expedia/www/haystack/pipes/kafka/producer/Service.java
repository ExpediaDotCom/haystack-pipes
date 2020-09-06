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
package com.expedia.www.haystack.pipes.kafka.producer;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.expedia.www.haystack.pipes.kafka.producer.config.KafkaProducerConfig;
import com.expedia.www.haystack.pipes.kafka.producer.key.extractor.JsonExtractor;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.key.extractor.loader.SpanKeyExtractorLoader;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class Service {

    private static final SerdeFactory serdeFactory = new SerdeFactory();
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final ProjectConfiguration projectConfiguration = ProjectConfiguration.getInstance();
    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(Service.class);
    @VisibleForTesting
    static HealthController healthController = new HealthController();
    @VisibleForTesting
    static Service service = null;
    @VisibleForTesting
    private static Map<SpanKeyExtractor, List<KafkaProducer<String, String>>> extractorListMap = null;


    private Service() {
    }

    public static void main(String[] args) {
        logger.info("Initializing Kafka Consumers");
        service = Service.getInstance();
        service.inPlaceHealthCheck();
        JmxReporter jmxReporter = service.getJmxReporter();
        jmxReporter.start();
        final ProtobufToKafkaProducer protobufToKafkaProducer = service.getProtobufToKafkaProducer(service.getKafkaStreamStarter());
        protobufToKafkaProducer.main();
    }

    public static Service getInstance() {
        if (null == service) {
            service = new Service();
        }
        return service;
    }

    public static Map<SpanKeyExtractor, List<KafkaProducer<String, String>>> getExtractorKafkaProducerMap(ProjectConfiguration projectConfiguration) {

        if (null == extractorListMap) {
            List<KafkaProducerConfig> kafkaProducerConfigs = projectConfiguration.getKafkaProducerConfigs();
            KafkaToKafkaPipeline.Factory factory = new KafkaToKafkaPipeline.Factory();
            Map<String, KafkaProducer<String, String>> kafkaProducerNameMap = new HashMap<>();
            kafkaProducerConfigs.forEach(kafkaProducerConfig -> {
                kafkaProducerNameMap.put(kafkaProducerConfig.getName(), factory.createKafkaProducer(kafkaProducerConfig.getConfigurationMap()));
            });
            List<SpanKeyExtractor> spanKeyExtractors = SpanKeyExtractorLoader.getInstance().getSpanKeyExtractor(projectConfiguration.getSpanExtractorConfigs());
            SpanKeyExtractor defaultExtractor = new JsonExtractor();// default extractor for backward compatibility
            defaultExtractor.configure(projectConfiguration.getSpanExtractorConfigs().get(defaultExtractor.name()));
            spanKeyExtractors.add(defaultExtractor);// adding default extractor for backward compatibility

            extractorListMap = spanKeyExtractors.stream()
                    .collect(Collectors.toMap(
                            spanKeyExtractor -> spanKeyExtractor,
                            spanKeyExtractor -> {
                                List<KafkaProducer<String, String>> list = new ArrayList<>();
                                spanKeyExtractor.getProducers().forEach(producerStr -> {
                                    list.add(kafkaProducerNameMap.get(producerStr));
                                });
                                return list;
                            }
                    ));
        }

        return extractorListMap;
    }

    public ProtobufToKafkaProducer getProtobufToKafkaProducer(KafkaStreamStarter kafkaStreamStarter) {
        return new ProtobufToKafkaProducer(kafkaStreamStarter,
                serdeFactory, getKafkaToKafkaPipeline(),
                projectConfiguration.getKafkaConsumerConfig());
    }

    public KafkaStreamStarter getKafkaStreamStarter() {
        return new KafkaStreamStarter(ProtobufToKafkaProducer.class,
                Constants.APPLICATION, projectConfiguration.getKafkaConsumerConfig(),
                healthController);
    }

    private KafkaToKafkaPipeline getKafkaToKafkaPipeline() {
        return new KafkaToKafkaPipeline(metricRegistry,
                getExtractorKafkaProducerMap(projectConfiguration));
    }

    public void inPlaceHealthCheck() {
        healthController.addListener(new UpdateHealthStatusFile("/app/isHealthy"));
    }

    public JmxReporter getJmxReporter() {
        return JmxReporter.forRegistry(metricRegistry).build();
    }


}
