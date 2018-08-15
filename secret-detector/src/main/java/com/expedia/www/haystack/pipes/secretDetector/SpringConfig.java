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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.expedia.www.haystack.commons.secretDetector.span.SpanDetector;
import com.expedia.www.haystack.commons.secretDetector.span.SpanNameAndCountRecorder;
import com.expedia.www.haystack.commons.secretDetector.span.SpanS3ConfigFetcher;
import com.expedia.www.haystack.commons.secretDetector.span.SpanSecretMasker;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.HealthStatusListener;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction;
import com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedActionFactory;
import com.expedia.www.haystack.pipes.secretDetector.actions.SenderImpl;
import com.expedia.www.haystack.pipes.secretDetector.config.ActionsConfigurationProvider;
import com.expedia.www.haystack.pipes.secretDetector.config.SecretsEmailConfigurationProvider;
import com.expedia.www.haystack.pipes.secretDetector.config.SpringWiredWhiteListConfigurationProvider;
import com.expedia.www.haystack.pipes.secretDetector.mains.ProtobufSpanMaskerToKafkaTransformer;
import com.expedia.www.haystack.pipes.secretDetector.mains.ProtobufSpanToEmailInKafkaTransformer;
import com.expedia.www.haystack.pipes.secretDetector.mains.ProtobufToDetectorAction;
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

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SPAN_ARRIVAL_TIMER_NAME;
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
    ProtobufToDetectorAction detectorProducer(KafkaStreamStarter kafkaStreamStarter,
                                              SerdeFactory serdeFactory,
                                              DetectorAction detectorAction,
                                              KafkaConfigurationProvider kafkaConfigurationProvider) {
        return new ProtobufToDetectorAction(kafkaStreamStarter, serdeFactory, detectorAction, kafkaConfigurationProvider);
    }

    @Bean
    @Autowired
    ProtobufSpanToEmailInKafkaTransformer protobufSpanToEmailInKafkaTransformer(KafkaStreamStarter kafkaStreamStarter,
                                                                                SerdeFactory serdeFactory,
                                                                                SpanDetector spanDetector) {
        return new ProtobufSpanToEmailInKafkaTransformer(kafkaStreamStarter, serdeFactory, spanDetector);
    }

    @Bean
    @Autowired
    ProtobufSpanMaskerToKafkaTransformer protobufSpanMaskerToKafkaTransformer(KafkaStreamStarter kafkaStreamStarter,
                                                                              SerdeFactory serdeFactory,
                                                                              SpanSecretMasker spanSecretMasker) {
        return new ProtobufSpanMaskerToKafkaTransformer(kafkaStreamStarter, serdeFactory, spanSecretMasker);
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
    Logger emailerDetectedActionLogger() {
        return LoggerFactory.getLogger(EmailerDetectedAction.class);
    }

    @Bean
    Logger detectorLogger() {
        return LoggerFactory.getLogger(SpanDetector.class);
    }

    @Bean
    Logger spanS3ConfigFetcherLogger() {
        return LoggerFactory.getLogger(SpanS3ConfigFetcher.class);
    }

    @Bean
    Logger spanNameAndCountRecorderLogger() {
        return LoggerFactory.getLogger(SpanNameAndCountRecorder.class);
    }

    @Bean
    AmazonS3 amazonS3Client() {
        return AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
    }

    @Bean
    SpanS3ConfigFetcher.SpanFactory s3ConfigFetcherFactory() {
        return new SpanS3ConfigFetcher.SpanFactory();
    }

    @Bean
    @Autowired
    KafkaStreamStarter kafkaStreamStarter(final HealthController healthController) {
        return new KafkaStreamStarter(ProtobufToDetectorAction.class, APPLICATION, healthController);
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
    Timer spanArrivalTimer() {
        return metricObjects.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION,
                DetectorAction.class.getSimpleName(), SPAN_ARRIVAL_TIMER_NAME, TimeUnit.MILLISECONDS);
    }

    @Bean
    SerdeFactory serdeFactory() {
        return new SerdeFactory();
    }

    @Bean
    DetectorIsActiveController.Factory detectorIsActiveControllerFactory() {
        return new DetectorIsActiveController.Factory();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    @Autowired
    CountersAndTimer countersAndTimer(Clock clock,
                                      Counter detectorActionRequestCounter,
                                      Timer detectorDetectTimer,
                                      Timer spanArrivalTimer) {
        return new CountersAndTimer(clock, detectorDetectTimer, spanArrivalTimer, detectorActionRequestCounter);
    }

    @Bean
    FinderEngine finderEngine() {
        return new FinderEngine();
    }

    @Bean
    SpanDetector.Factory detectorFactory(MetricObjects metricObjects) {
        return new SpanDetector.Factory(metricObjects);
    }

    @Bean
    @Autowired
    SpringWiredDetector spanDetector(Logger detectorLogger,
                                     FinderEngine finderEngine,
                                     SpanDetector.Factory detectorFactory,
                                     SpanS3ConfigFetcher s3ConfigFetcher) {
        return new SpringWiredDetector(detectorLogger, finderEngine, detectorFactory, s3ConfigFetcher);
    }

    @Bean
    @Autowired
    SpanSecretMasker.Factory spanSecretMasterFactory(MetricObjects metricObjects) {
        return new SpanSecretMasker.Factory(metricObjects);
    }

    @Bean
    @Autowired
    DetectorAction detectorAction(CountersAndTimer detectorDetectTimer,
                                  SpanDetector spanDetector,
                                  Logger detectorActionLogger,
                                  ActionsConfigurationProvider actionsConfigurationProvider) {
        return new DetectorAction(detectorDetectTimer, spanDetector, detectorActionLogger, actionsConfigurationProvider);
    }

    @Bean
    EmailerDetectedAction.MimeMessageFactory emailerDetectedActionFactory() {
        return new EmailerDetectedAction.MimeMessageFactory();
    }

    @Bean
    EmailerDetectedAction.Sender sender() {
        return new SenderImpl();
    }

    @Bean
    EmailerDetectedAction.MimeMessageFactory mimeMessageFactory() {
        return new EmailerDetectedAction.MimeMessageFactory();
    }

    @Bean
    @Autowired
    EmailerDetectedActionFactory emailerDetectedActionFactory(EmailerDetectedAction.MimeMessageFactory mimeMessageFactory,
                                                              Logger emailerDetectedActionLogger,
                                                              EmailerDetectedAction.Sender sender,
                                                              SecretsEmailConfigurationProvider secretsEmailConfigurationProvider) {
        return new EmailerDetectedActionFactory(mimeMessageFactory, emailerDetectedActionLogger,
                sender, secretsEmailConfigurationProvider);
    }

    @Bean
    @Autowired
    EmailerDetectedAction emailerDetectedAction(EmailerDetectedAction.MimeMessageFactory mimeMessageFactory,
                                                Logger emailerDetectedActionLogger,
                                                EmailerDetectedAction.Sender sender,
                                                SecretsEmailConfigurationProvider secretsEmailConfigurationProvider) {
        return new EmailerDetectedAction(mimeMessageFactory,
                emailerDetectedActionLogger,
                sender,
                secretsEmailConfigurationProvider);
    }

    @Bean
    @Autowired
    SpanS3ConfigFetcher s3ConfigFetcher(Logger spanS3ConfigFetcherLogger,
                                        SpringWiredWhiteListConfigurationProvider whiteListConfig,
                                        AmazonS3 amazonS3,
                                        SpanS3ConfigFetcher.SpanFactory s3ConfigFetcherFactory) {
        return new SpanS3ConfigFetcher(spanS3ConfigFetcherLogger, whiteListConfig, amazonS3, s3ConfigFetcherFactory);
    }

    @Bean
    @Autowired
    SpanNameAndCountRecorder spanNameAndCountRecorder(Logger spanNameAndCountRecorderLogger) {
        return new SpanNameAndCountRecorder(spanNameAndCountRecorderLogger, Clock.systemUTC());
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
            final com.expedia.www.haystack.commons.config.Configuration configuration
                    = new com.expedia.www.haystack.commons.config.Configuration();
            return configuration.createMergeConfigurationProvider();
        }

        @Bean
        @Autowired
        ActionsConfigurationProvider actionsConfigurationProvider(Logger actionsConfigurationProviderLogger,
                                                                  ConfigurationProvider configurationProvider) {
            return new ActionsConfigurationProvider(actionsConfigurationProviderLogger, configurationProvider);
        }

        @Bean
        SecretsEmailConfigurationProvider secretsEmailConfigurationProvider() {
            return new SecretsEmailConfigurationProvider();
        }

        @Bean
        KafkaConfigurationProvider kafkaConfigurationProvider() {
            return new KafkaConfigurationProvider();
        }
    }
}
