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
package com.expedia.www.haystack.pipes.firehoseWriter;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.firehoseWriter.Constants.APPLICATION;

/**
 * Spring configuration class. The FirehoseIsActiveControllerTest.testMainWithMockObjects() unit test creates a new
 * AnnotationConfigApplicationContext that loads this class and instantiates all Spring beans defined in it,
 * so less than 100% unit test coverage of this class indicates a Spring configuration bean that is not used.
 */
@Configuration
@ComponentScan(basePackageClasses = SpringConfig.class)
class SpringConfig {
    private final MetricObjects metricObjects;

    /**
     * @param metricObjects provided by a static inner class that is loaded first
     * @see MetricObjectsSpringConfig
     */
    @Autowired
    SpringConfig(MetricObjects metricObjects) {
        this.metricObjects = metricObjects;
    }

    // Beans with unit tests
    @Bean
    Counter requestCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseAction.class.getName(), "REQUEST");
    }

    @Bean
    KafkaStreamStarter kafkaStreamStarter() {
        return new KafkaStreamStarter(ProtobufToFirehoseProducer.class, APPLICATION);
    }

    @Bean
    Logger firehoseActionLogger() {
        return LoggerFactory.getLogger(FirehoseAction.class);
    }

    @Bean
    Logger protobufToFirehoseProducerLogger() {
        return LoggerFactory.getLogger(ProtobufToFirehoseProducer.class);
    }

    @Bean
    Logger firehoseIsActiveControllerLogger() {
        return LoggerFactory.getLogger(FirehoseIsActiveController.class);
    }

    @Bean
    EndpointConfiguration endpointConfiguration(String url,
                                                String signingregion) {
        return new EndpointConfiguration(url, signingregion);
    }

    @Bean
    String url(FirehoseConfigurationProvider firehoseConfigurationProvider) {
        return firehoseConfigurationProvider.url();
    }

    @Bean
    String signingregion(FirehoseConfigurationProvider firehoseConfigurationProvider) {
        return firehoseConfigurationProvider.signingregion();
    }

    @Bean
    ClientConfiguration clientConfiguration() {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setUseGzip(true);
        return clientConfiguration;
    }

    // Beans without unit tests
    @Bean
    @Autowired
    AmazonKinesisFirehose amazonKinesisFirehose(EndpointConfiguration endpointConfiguration,
                                                ClientConfiguration clientConfiguration) {
        return AmazonKinesisFirehoseClientBuilder
                .standard()
                .withEndpointConfiguration(endpointConfiguration)
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    @Bean
    FirehoseCollector firehoseCollector() {
        return new FirehoseCollector();
    }

    @Bean
    SpanSerdeFactory spanSerdeFactory() {
        return new SpanSerdeFactory();
    }

    @Bean
    FirehoseIsActiveController.Factory firehoseIsActiveControllerFactory() {
        return new FirehoseIsActiveController.Factory();
    }

    @Bean
    FirehoseConfigurationProvider firehoseConfigurationProvider() {
        return new FirehoseConfigurationProvider();
    }

    @Bean
    @Autowired
    FirehoseAction firehoseAction(Printer printer,
                                  Logger firehoseActionLogger,
                                  Counter requestCounter,
                                  FirehoseCollector firehoseCollector,
                                  AmazonKinesisFirehose amazonKinesisFirehose) {
        return new FirehoseAction(printer, firehoseActionLogger, requestCounter, firehoseCollector,
                amazonKinesisFirehose);
    }

    @Bean
    KafkaConfigurationProvider kafkaConfigurationProvider() {
        return new KafkaConfigurationProvider();
    }

    @Bean
    @Autowired
    ProtobufToFirehoseProducer protobufToFirehoseProducer(KafkaStreamStarter kafkaStreamStarter,
                                                          SpanSerdeFactory spanSerdeFactory,
                                                          FirehoseAction firehoseAction,
                                                          KafkaConfigurationProvider kafkaConfigurationProvider) {
        return new ProtobufToFirehoseProducer(
                kafkaStreamStarter, spanSerdeFactory, firehoseAction, kafkaConfigurationProvider);
    }

    @Bean
    Printer printer() {
        return JsonFormat.printer().omittingInsignificantWhitespace();
    }

    /**
     * Spring loads this static inner class before loading the SpringConfig outer class so that its bean is available to
     * the outer class constructor.
     *
     * @see SpringConfig
     */
    @Configuration
    static class MetricObjectsSpringConfig {
        @Bean
        public MetricObjects metricObjects() {
            return new MetricObjects();
        }
    }
}

