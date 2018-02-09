package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import com.netflix.servo.monitor.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

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
    KafkaStreamStarter kafkaStreamStarter() {
        return new KafkaStreamStarter(ProtobufToKafkaProducer.class, APPLICATION);
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
    KafkaProducerIsActiveController.Factory kafkaProducerIsActiveControllerFactory() {
        return new KafkaProducerIsActiveController.Factory();
    }

    @Bean
    @Autowired
    ProduceIntoExternalKafkaAction produceIntoExternalKafkaAction(
            Counter produceIntoExternalKafkaActionRequestCounter,
            Counter postsInFlightCounter) {
        return new ProduceIntoExternalKafkaAction(produceIntoExternalKafkaActionRequestCounter, postsInFlightCounter);
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
