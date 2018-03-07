package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

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
    KafkaStreamStarter kafkaStreamStarter() {
        return new KafkaStreamStarter(ProtobufToHttpPoster.class, APPLICATION);
    }

    @Bean
    Logger httpPostIsActiveControllerLogger() {
        return LoggerFactory.getLogger(HttpPostIsActiveController.class);
    }

    @Bean
    Logger httpPostActionLogger() {
        return LoggerFactory.getLogger(HttpPostAction.class);
    }

    // Beans without unit tests
    @Bean
    HttpPostConfigurationProvider httpPostConfigurationProvider() {
        return new HttpPostConfigurationProvider();
    }

    @Autowired
    @Bean
    ContentCollector contentCollector(HttpPostConfigurationProvider httpPostConfigurationProvider) {
        return new ContentCollector(httpPostConfigurationProvider);
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
    SpanSerdeFactory spanSerdeFactory() {
        return new SpanSerdeFactory();
    }

    @Bean
    KafkaConfigurationProvider kafkaConfigurationProvider() {
        return new KafkaConfigurationProvider();
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
    @Autowired
    CountersAndTimer countersAndTimer(Counter requestCounter,
                                      Counter filteredOutCounter,
                                      Counter filteredInCounter,
                                      Timer httpPostTimer) {
        return new CountersAndTimer(httpPostTimer, requestCounter, filteredOutCounter, filteredInCounter);
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
