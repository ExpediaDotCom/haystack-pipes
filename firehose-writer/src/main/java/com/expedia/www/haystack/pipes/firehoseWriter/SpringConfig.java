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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.util.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    @VisibleForTesting
    static final long[] BUCKETS_FOR_PUT_BATCH_REQUEST_TIMER = {1L, 2L, 5L, 10L, 20L, 50L, 100L, 200L, 500L,
            1_000L, 2_000L, 5_000L, 10_000L, 20_000L, 50_000L, 100_000L, 200_000L, 500_000L};
    private final MetricObjects metricObjects;

    /**
     * @param metricObjects provided by a static inner class that is loaded before this SpringConfig class
     * @see MetricObjectsSpringConfig
     */
    @Autowired
    SpringConfig(MetricObjects metricObjects) {
        this.metricObjects = metricObjects;
    }

    // Beans with unit tests ///////////////////////////////////////////////////////////////////////////////////////////
    @Bean
    Counter spanCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), "REQUEST");
    }

    @Bean
    Counter successCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), "SUCCESS");
    }

    @Bean
    Counter failureCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), "FAILURE");
    }

    @Bean
    Counter exceptionCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseProcessor.class.getName(), "EXCEPTION");
    }

    @Bean
    Counter throttledCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                Batch.class.getName(), "THROTTLED");
    }

    @Bean
    Timer putBatchRequestTimer() {
        return metricObjects.createAndRegisterBucketTimer(SUBSYSTEM, APPLICATION, "PUT_BATCH_REQUEST",
                TimeUnit.MILLISECONDS, BUCKETS_FOR_PUT_BATCH_REQUEST_TIMER);
    }

    @Bean
    @Autowired
    KafkaStreamStarter kafkaStreamStarter(final HealthController healthController) {
        return new KafkaStreamStarter(ProtobufToFirehoseProducer.class, APPLICATION, healthController);
    }

    @Bean
    HealthController healthController() {
        final HealthController healthController = new HealthController();
        healthController.addListener(new UpdateHealthStatusFile("/app/isHealthy")); // TODO should come from config
        return healthController;
    }

    @Bean
    Logger firehoseProcessorLogger() {
        return LoggerFactory.getLogger(FirehoseProcessor.class);
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
    Logger batchLogger() {
        return LoggerFactory.getLogger(Batch.class);
    }

    @Bean
    @Autowired
    EndpointConfiguration endpointConfiguration(String url,
                                                String signingregion) {
        return new EndpointConfiguration(url, signingregion);
    }

    @Bean
    @Autowired
    String url(FirehoseConfigurationProvider firehoseConfigurationProvider) {
        return firehoseConfigurationProvider.url();
    }

    @Bean
    @Autowired
    String signingregion(FirehoseConfigurationProvider firehoseConfigurationProvider) {
        return firehoseConfigurationProvider.signingregion();
    }

    @Bean
    ClientConfiguration clientConfiguration() {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setUseGzip(true);
        return clientConfiguration;
    }

    // Beans without unit tests ////////////////////////////////////////////////////////////////////////////////////////
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
    Supplier<FirehoseCollector> firehoseCollector(FirehoseConfigurationProvider configurationProvider) {
        return () -> configurationProvider.usestringbuffering() ?
                new FirehoseStringBufferCollector(configurationProvider.maxbatchinterval()) : new FirehoseRecordBufferCollector();
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
    FirehoseProcessor.Factory firehoseProcessorFactory() {
        return new FirehoseProcessor.Factory();
    }

    @Bean
    @Autowired
    FirehoseCountersAndTimer counters(Timer putBatchRequestTimer,
                                      Counter spanCounter,
                                      Counter successCounter,
                                      Counter failureCounter,
                                      Counter exceptionCounter) {
        return new FirehoseCountersAndTimer(
                putBatchRequestTimer, spanCounter, successCounter, failureCounter, exceptionCounter);
    }

    @Bean
    @Autowired
    Supplier<Batch> batch(Printer printer,
                          Supplier<FirehoseCollector> firehoseCollector,
                          Logger batchLogger,
                          Counter throttledCounter) {
        return () -> new Batch(printer, firehoseCollector, batchLogger, throttledCounter);
    }

    @Bean
    @Autowired
    FirehoseProcessorSupplier firehoseProcessorSupplier(Logger firehoseProcessorLogger,
                                                        FirehoseCountersAndTimer firehoseCountersAndTimer,
                                                        Supplier<Batch> batch,
                                                        AmazonKinesisFirehose amazonKinesisFirehose,
                                                        FirehoseProcessor.Factory firehoseProcessorFactory,
                                                        FirehoseConfigurationProvider firehoseConfigurationProvider) {
        return new FirehoseProcessorSupplier(firehoseProcessorLogger, firehoseCountersAndTimer, batch,
                amazonKinesisFirehose, firehoseProcessorFactory, firehoseConfigurationProvider);
    }

    @Bean
    KafkaConfigurationProvider kafkaConfigurationProvider() {
        return new KafkaConfigurationProvider();
    }

    @Bean
    @Autowired
    ProtobufToFirehoseProducer protobufToFirehoseProducer(KafkaStreamStarter kafkaStreamStarter,
                                                          SpanSerdeFactory spanSerdeFactory,
                                                          FirehoseProcessorSupplier firehoseProcessorSupplier,
                                                          KafkaConfigurationProvider kafkaConfigurationProvider) {
        return new ProtobufToFirehoseProducer(
                kafkaStreamStarter, spanSerdeFactory, firehoseProcessorSupplier, kafkaConfigurationProvider);
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

