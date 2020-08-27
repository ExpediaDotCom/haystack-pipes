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
package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.Timers;
import com.expedia.www.haystack.pipes.commons.TimersAndCounters;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.kafka.config.HttpPostConfig;
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.expedia.www.haystack.pipes.commons.kafka.config.ProjectConfiguration;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SPAN_ARRIVAL_TIMER_NAME;
import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.httpPoster.Constants.APPLICATION;

@Configuration
@ComponentScan(basePackageClasses = SpringConfig.class)
public class SpringConfig {
    @VisibleForTesting
    static final String HTTP_POST_ACTION_CLASS_SIMPLE_NAME = HttpPostAction.class.getSimpleName();
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
                HTTP_POST_ACTION_CLASS_SIMPLE_NAME, "REQUEST");
    }

    @Bean
    Counter filteredOutCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                HTTP_POST_ACTION_CLASS_SIMPLE_NAME, "FILTERED_OUT");
    }

    @Bean
    Counter filteredInCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                HTTP_POST_ACTION_CLASS_SIMPLE_NAME, "FILTERED_IN");
    }

    @Bean
    Timer httpPostTimer() {
        return metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                HTTP_POST_ACTION_CLASS_SIMPLE_NAME, "HTTP_POST", TimeUnit.MICROSECONDS);
    }

    @Bean
    Timer spanArrivalTimer() {
        return metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                HTTP_POST_ACTION_CLASS_SIMPLE_NAME, SPAN_ARRIVAL_TIMER_NAME, TimeUnit.MILLISECONDS);
    }

    @Bean
    @Autowired
    KafkaStreamStarter kafkaStreamStarter(final HealthController healthController) {
        return new KafkaStreamStarter(ProtobufToHttpPoster.class, APPLICATION, healthController);
    }

    @Bean
    HealthController healthController() {
        final HealthController healthController = new HealthController();
        healthController.addListener(new UpdateHealthStatusFile("/app/isHealthy")); // TODO should come from config
        return healthController;
    }

    @Bean
    Logger httpPostIsActiveControllerLogger() {
        return LoggerFactory.getLogger(HttpPostIsActiveController.class);
    }

    @Bean
    Logger httpPostActionLogger() {
        return LoggerFactory.getLogger(HttpPostAction.class);
    }

    @Bean
    Logger invalidProtocolBufferExceptionLoggerLogger() {
        return LoggerFactory.getLogger(InvalidProtocolBufferExceptionLogger.class);
    }

    // Beans without unit tests
    @Bean
    InvalidProtocolBufferExceptionLogger invalidProtocolBufferExceptionLogger(
            Logger invalidProtocolBufferExceptionLoggerLogger) {
        return new InvalidProtocolBufferExceptionLogger(invalidProtocolBufferExceptionLoggerLogger);
    }

    @Bean
    HttpPostConfig httpPostConfig() {
        return ProjectConfiguration.getInstance().getHttpPostConfig();
    }

    @Autowired
    @Bean
    ContentCollector contentCollector(HttpPostConfig httpPostConfig) {
        return new ContentCollector(httpPostConfig);
    }

    @Bean
    HttpPostIsActiveController.Factory httpPostIsActiveControllerFactory() {
        return new HttpPostIsActiveController.Factory();
    }

    @Bean
    HttpPostAction.Factory httpPostActionFactory() {
        return new HttpPostAction.Factory();
    }

    @Bean
    SerdeFactory serdeFactory() {
        return new SerdeFactory();
    }

    @Bean
    KafkaConsumerConfig kafkaConsumerConfig() {
        return ProjectConfiguration.getInstance().getKafkaConsumerConfig();
    }

    @Bean
    JsonFormat.Printer printer() {
        return JsonFormat.printer().omittingInsignificantWhitespace();
    }

    @Bean
    Random random() {
        return new Random();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    @Autowired
    Timers timers(Timer httpPostTimer,
                  Timer spanArrivalTimer) {
        return new Timers(httpPostTimer, spanArrivalTimer);
    }

    @Bean
    @Autowired
    TimersAndCounters countersAndTimer(Clock clock,
                                       Counter requestCounter,
                                       Counter filteredOutCounter,
                                       Counter filteredInCounter,
                                       Timers timers) {
        return new TimersAndCounters(clock, timers, requestCounter, filteredOutCounter, filteredInCounter);
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
