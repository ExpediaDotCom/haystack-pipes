package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.kafkaProducer.Constants.APPLICATION;

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
                ProduceIntoExternalKafkaAction.class.getSimpleName(), "REQUEST");
    }

    @Bean
    Counter postsInFlightCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                ProduceIntoExternalKafkaAction.class.getSimpleName(), "POSTS_IN_FLIGHT");
    }

    @Bean
    Logger produceIntoExternalKafkaCallbackLogger() {
        return LoggerFactory.getLogger(ProduceIntoExternalKafkaCallback.class);
    }

    @Bean
    Logger produceIntoExternalKafkaActionLogger() {
        return LoggerFactory.getLogger(ProduceIntoExternalKafkaAction.class);
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
                ProduceIntoExternalKafkaAction.class.getSimpleName(), "KAFKA_PRODUCER_POST", TimeUnit.MICROSECONDS);
    }

    // Beans without unit tests ////////////////////////////////////////////////////////////////////////////////////////
    @Bean
    SpanSerdeFactory spanSerdeFactory() {
        return new SpanSerdeFactory();
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
    KafkaProducerIsActiveController.Factory kafkaProducerIsActiveControllerFactory() {
        return new KafkaProducerIsActiveController.Factory();
    }

    @Bean
    ProduceIntoExternalKafkaAction.Factory produceIntoExternalKafkaActionFactory() {
        return new ProduceIntoExternalKafkaAction.Factory();
    }

    @Bean
    @Autowired
    CountersAndTimer countersAndTimer(Counter produceIntoExternalKafkaActionRequestCounter,
                                      Counter postsInFlightCounter,
                                      Timer kafkaProducerPost) {
        return new CountersAndTimer(
                kafkaProducerPost, produceIntoExternalKafkaActionRequestCounter, postsInFlightCounter);
    }
    @Bean
    @Autowired
    ProduceIntoExternalKafkaAction produceIntoExternalKafkaAction(
            ProduceIntoExternalKafkaAction.Factory produceIntoExternalKafkaActionFactoryFactory,
            CountersAndTimer countersAndTimer,
            Logger produceIntoExternalKafkaActionLogger,
            ExternalKafkaConfigurationProvider externalKafkaConfigurationProvider) {
        return new ProduceIntoExternalKafkaAction(
                produceIntoExternalKafkaActionFactoryFactory,
                countersAndTimer,
                produceIntoExternalKafkaActionLogger,
                externalKafkaConfigurationProvider);
    }

    @Bean
    @Autowired
    ProtobufToKafkaProducer protobufToKafkaProducer(KafkaStreamStarter kafkaStreamStarter,
                                                    SpanSerdeFactory spanSerdeFactory,
                                                    ProduceIntoExternalKafkaAction produceIntoExternalKafkaAction,
                                                    KafkaConfigurationProvider kafkaConfigurationProvider) {
        return new ProtobufToKafkaProducer(
                kafkaStreamStarter, spanSerdeFactory, produceIntoExternalKafkaAction, kafkaConfigurationProvider);
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
