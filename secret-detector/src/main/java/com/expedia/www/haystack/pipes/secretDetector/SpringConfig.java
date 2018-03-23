package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.HealthStatusListener;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import io.dataapps.chlorine.finder.FinderEngine;
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
    DetectorIsActiveController detectorIsActiveController(DetectorProducer detectorProducer,
                                                          DetectorIsActiveController.Factory detectorIsActiveControllerFactory,
                                                          Logger detectorIsActiveControllerLogger) {
        return new DetectorIsActiveController(detectorProducer, detectorIsActiveControllerFactory,
                detectorIsActiveControllerLogger);
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
        return LoggerFactory.getLogger(Emailer.class);
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
                                  Logger detectorActionLogger) {
        return new DetectorAction(detectorDetectTimer, detector, detectorActionLogger);
    }

    @Bean
    SecretsConfigurationProvider secretsConfigurationProvider() {
        return new SecretsConfigurationProvider();
    }

    @Bean
    Emailer.Factory emailerFactory() {
        return new Emailer.Factory();
    }

    @Bean
    Emailer.Sender sender() {
        return new SenderImpl();
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
