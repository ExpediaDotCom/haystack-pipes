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
package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.HealthStatusListener;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import com.expedia.www.haystack.pipes.secretDetector.com.expedia.www.haystack.pipes.secretDetector.actions.ActionsConfigurationProvider;
import com.expedia.www.haystack.pipes.secretDetector.com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction;
import com.expedia.www.haystack.pipes.secretDetector.com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedActionFactory;
import com.expedia.www.haystack.pipes.secretDetector.com.expedia.www.haystack.pipes.secretDetector.actions.SenderImpl;
import com.expedia.www.haystack.pipes.secretDetector.mains.DetectorProducer;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import io.dataapps.chlorine.finder.FinderEngine;
import org.cfg4j.provider.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;

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

    @Bean
    @Autowired
    DetectorProducer detectorProducer(KafkaStreamStarter kafkaStreamStarter,
                                      SpanSerdeFactory spanSerdeFactory,
                                      DetectorAction detectorAction,
                                      KafkaConfigurationProvider kafkaConfigurationProvider) {
        return new DetectorProducer(kafkaStreamStarter, spanSerdeFactory, detectorAction, kafkaConfigurationProvider);
    }

    @Bean
    @Autowired
    DetectorIsActiveController detectorIsActiveController(DetectorIsActiveController.Factory detectorIsActiveControllerFactory,
                                                          Logger detectorIsActiveControllerLogger,
                                                          ActionsConfigurationProvider actionsConfigurationProvider) {
        return new DetectorIsActiveController(
                detectorIsActiveControllerFactory, detectorIsActiveControllerLogger, actionsConfigurationProvider);
    }
    @Bean
    Logger detectorIsActiveControllerLogger() {
        return LoggerFactory.getLogger(DetectorIsActiveController.class);
    }

    @Bean
    Logger detectorActionLogger() {
        return LoggerFactory.getLogger(DetectorAction.class);
    }

    @Bean
    Logger emailerLogger() {
        return LoggerFactory.getLogger(EmailerDetectedAction.class);
    }

    @Bean
    @Autowired
    KafkaStreamStarter kafkaStreamStarter(final HealthController healthController) {
        return new KafkaStreamStarter(DetectorProducer.class, APPLICATION, healthController);
    }

    @Bean
    HealthStatusListener healthStatusListener() {
        return new UpdateHealthStatusFile("/app/isHealthy"); // TODO should come from config
    }

    @Bean
    @Autowired
    HealthController healthController(HealthStatusListener healthStatusListener) {
        final HealthController healthController = new HealthController();
        healthController.addListener(healthStatusListener);
        return healthController;
    }

    @Bean
    Counter detectorActionRequestCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                DetectorAction.class.getSimpleName(), "DETECTOR_SPAN");
    }

    @Bean
    Timer detectorDetectTimer() {
        return metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                DetectorAction.class.getSimpleName(), "DETECTOR_DETECT", TimeUnit.MICROSECONDS);
    }

    @Bean
    SpanSerdeFactory spanSerdeFactory() {
        return new SpanSerdeFactory();
    }

    @Bean
    KafkaConfigurationProvider kafkaConfigurationProvider() {
        return new KafkaConfigurationProvider();
    }

    @Bean
    DetectorIsActiveController.Factory kafkaProducerIsActiveControllerFactory() {
        return new DetectorIsActiveController.Factory();
    }

    @Bean
    @Autowired
    CountersAndTimer countersAndTimer(Counter detectorActionRequestCounter,
                                      Timer detectorDetectTimer) {
        return new CountersAndTimer(detectorDetectTimer, detectorActionRequestCounter);
    }

    @Bean
    FinderEngine finderEngine() {
        return new FinderEngine();
    }

    @Bean
    @Autowired
    Detector detector(FinderEngine finderEngine) {
        return new Detector(finderEngine);
    }

    @Bean
    @Autowired
    DetectorAction detectorAction(CountersAndTimer detectorDetectTimer,
                                  Detector detector,
                                  Logger detectorActionLogger,
                                  ActionsConfigurationProvider actionsConfigurationProvider) {
        return new DetectorAction(detectorDetectTimer, detector, detectorActionLogger, actionsConfigurationProvider);
    }

    @Bean
    SecretsEmailConfigurationProvider secretsEmailConfigurationProvider() {
        return new SecretsEmailConfigurationProvider();
    }

    @Bean
    EmailerDetectedAction.Factory emailerFactory() {
        return new EmailerDetectedAction.Factory();
    }

    @Bean
    EmailerDetectedAction.Sender sender() {
        return new SenderImpl();
    }

    @Bean
    @Autowired
    EmailerDetectedActionFactory emailerDetectedActionFactory(EmailerDetectedAction.Factory emailerFactory,
                                                              Logger emailerLogger,
                                                              EmailerDetectedAction.Sender sender,
                                                              SecretsEmailConfigurationProvider secretsEmailConfigurationProvider) {
        return new EmailerDetectedActionFactory(emailerFactory, emailerLogger, sender, secretsEmailConfigurationProvider);
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

    /*
     * Spring loads this static inner class before loading the SpringConfig outer class so that its beans are available
     * to the outer class constructor.
     *
     * @see SpringConfig
     */
    @Configuration
    static class ActionsConfigurationProviderSpringConfig {
        @Bean
        Logger actionsConfigurationProviderLogger() {
            return LoggerFactory.getLogger(ActionsConfigurationProvider.class);
        }

        @Bean
        ConfigurationProvider configurationProvider() {
            final com.expedia.www.haystack.pipes.commons.Configuration configuration
                    = new com.expedia.www.haystack.pipes.commons.Configuration();
            return configuration.createMergeConfigurationProvider();
        }

        @Bean
        @Autowired
        ActionsConfigurationProvider actionsConfigurationProvider(Logger actionsConfigurationProviderLogger,
                                                                  ConfigurationProvider configurationProvider) {
            return new ActionsConfigurationProvider(actionsConfigurationProviderLogger, configurationProvider);
        }

    }
}
