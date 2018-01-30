package com.expedia.www.haystack.pipes.httpPoster;

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
import static com.expedia.www.haystack.pipes.httpPoster.Constants.APPLICATION;

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

    // Beans with unit tests
    @Bean
    Counter requestCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                HttpPostAction.class.getName(), "REQUEST");
    }

    @Bean
    KafkaStreamStarter kafkaStreamStarter() {
        return new KafkaStreamStarter(ProtobufToHttpPoster.class, APPLICATION);
    }

    @Bean
    Logger httpPostIsActiveControllerLogger() {
        return LoggerFactory.getLogger(HttpPostIsActiveController.class);
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
    SpanSerdeFactory spanSerdeFactory() {
        return new SpanSerdeFactory();
    }

    @Bean
    KafkaConfigurationProvider kafkaConfigurationProvider() {
        return new KafkaConfigurationProvider();
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
