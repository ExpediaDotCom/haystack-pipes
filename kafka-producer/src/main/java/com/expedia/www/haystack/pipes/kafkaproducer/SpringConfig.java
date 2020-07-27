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
package com.expedia.www.haystack.pipes.kafkaproducer;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.Timers;
import com.expedia.www.haystack.pipes.commons.TimersAndCounters;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.config.SpanKeyExtractorConfigProvider;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SPAN_ARRIVAL_TIMER_NAME;
import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.kafkaproducer.Constants.APPLICATION;

@Configuration
@ComponentScan(basePackageClasses = SpringConfig.class)
public class SpringConfig {

    private final MetricObjects metricObjects;

    /**
     * @param metricObjects provided by a static inner class that is loaded first
     * @see MetricObjectsSpringConfig
     */
    @Autowired
    SpringConfig(MetricObjects metricObjects) {
        this.metricObjects = metricObjects;
    }

    // Beans with unit tests ///////////////////////////////////////////////////////////////////////////////////////////
    @Bean
    Counter produceIntoExternalKafkaActionRequestCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                KafkaToExternalKafkaAction.class.getSimpleName(), "REQUEST");
    }

    @Bean
    Counter postsInFlightCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                KafkaToExternalKafkaAction.class.getSimpleName(), "POSTS_IN_FLIGHT");
    }

    @Bean
    Logger produceIntoExternalKafkaCallbackLogger() {
        return LoggerFactory.getLogger(KafkaToExternalKafkaCallback.class);
    }

    @Bean
    Logger produceIntoExternalKafkaActionLogger() {
        return LoggerFactory.getLogger(KafkaToExternalKafkaAction.class);
    }

    @Bean
    Logger kafkaProducerIsActiveControllerLogger() {
        return LoggerFactory.getLogger(KafkaProducerIsActiveController.class);
    }

    @Bean
    @Autowired
    KafkaStreamStarter kafkaStreamStarter(final HealthController healthController) {
        return new KafkaStreamStarter(ProtobufToKafkaProducer.class, APPLICATION, healthController);
    }

    @Bean
    HealthController healthController() {
        final HealthController healthController = new HealthController();
        healthController.addListener(new UpdateHealthStatusFile("/app/isHealthy")); // TODO should come from config
        return healthController;
    }

    @Bean
    Timer kafkaProducerPost() {
        return metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                KafkaToExternalKafkaAction.class.getSimpleName(), "KAFKA_PRODUCER_POST", TimeUnit.MICROSECONDS);
    }

    @Bean
    Timer spanArrivalTimer() {
        return metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                KafkaToExternalKafkaAction.class.getSimpleName(), SPAN_ARRIVAL_TIMER_NAME, TimeUnit.MILLISECONDS);
    }

    // Beans without unit tests ////////////////////////////////////////////////////////////////////////////////////////
    @Bean
    SerdeFactory serdeFactory() {
        return new SerdeFactory();
    }

    @Bean
    @Autowired
    CallbackFactory callbackFactory(Logger produceIntoExternalKafkaCallbackLogger) {
        return new CallbackFactory(produceIntoExternalKafkaCallbackLogger);
    }

    @Bean
    KafkaConfigurationProvider kafkaConfigurationProvider() {
        return new KafkaConfigurationProvider();
    }

    @Bean
    ExternalKafkaConfigurationProvider externalKafkaConfigurationProvider() {
        return new ExternalKafkaConfigurationProvider();
    }

    @Bean
    SpanKeyExtractorConfigProvider spanKeyExtractorConfigProvider() {
        return new SpanKeyExtractorConfigProvider();
    }

    @Bean
    KafkaProducerIsActiveController.Factory kafkaProducerIsActiveControllerFactory() {
        return new KafkaProducerIsActiveController.Factory();
    }

    @Bean
    KafkaToExternalKafkaAction.Factory produceIntoExternalKafkaActionFactory() {
        return new KafkaToExternalKafkaAction.Factory();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    @Autowired
    Timers timers(Timer kafkaProducerPost,
                  Timer spanArrivalTimer) {
        return new Timers(kafkaProducerPost, spanArrivalTimer);
    }

    @Bean
    @Autowired
    TimersAndCounters countersAndTimer(Clock clock,
                                       Counter produceIntoExternalKafkaActionRequestCounter,
                                       Counter postsInFlightCounter,
                                       Timers timers) {
        return new TimersAndCounters(clock, timers,
                produceIntoExternalKafkaActionRequestCounter, postsInFlightCounter);
    }

    @Bean
    @Autowired
    KafkaToExternalKafkaAction produceIntoExternalKafkaAction(
            KafkaToExternalKafkaAction.Factory produceIntoExternalKafkaActionFactoryFactory,
            TimersAndCounters timersAndCounters,
            Logger produceIntoExternalKafkaActionLogger,
            ExternalKafkaConfigurationProvider externalKafkaConfigurationProvider,
            SpanKeyExtractorConfigProvider spanKeyExtractorConfigProvider) {
        return new KafkaToExternalKafkaAction(
                produceIntoExternalKafkaActionFactoryFactory,
                timersAndCounters,
                produceIntoExternalKafkaActionLogger,
                externalKafkaConfigurationProvider,
                spanKeyExtractorConfigProvider);
    }

    @Bean
    @Autowired
    ProtobufToKafkaProducer protobufToKafkaProducer(KafkaStreamStarter kafkaStreamStarter,
                                                    SerdeFactory serdeFactory,
                                                    KafkaToExternalKafkaAction produceIntoExternalKafkaAction,
                                                    KafkaConfigurationProvider kafkaConfigurationProvider) {
        return new ProtobufToKafkaProducer(
                kafkaStreamStarter, serdeFactory, produceIntoExternalKafkaAction, kafkaConfigurationProvider);
    }

    /*
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
