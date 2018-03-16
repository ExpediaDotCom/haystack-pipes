package com.expedia.www.haystack.pipes.jsonTransformer;

import com.expedia.www.haystack.pipes.commons.health.HealthController;
import com.expedia.www.haystack.pipes.commons.health.UpdateHealthStatusFile;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import static com.expedia.www.haystack.pipes.jsonTransformer.Constants.APPLICATION;

@Configuration
@ComponentScan(basePackageClasses = SpringConfig.class)
public class SpringConfig {
    // Beans with unit tests ///////////////////////////////////////////////////////////////////////////////////////////
    @Bean
    Logger jsonTransformerIsActiveControllerLogger() {
        return LoggerFactory.getLogger(JsonTransformerIsActiveController.class);
    }

    @Bean
    @Autowired
    KafkaStreamStarter kafkaStreamStarter(final HealthController healthController) {
        return new KafkaStreamStarter(ProtobufToJsonTransformer.class, APPLICATION, healthController);
    }

    @Bean
    HealthController healthController() {
        final HealthController healthController = new HealthController();
        healthController.addListener(new UpdateHealthStatusFile("/app/isHealthy")); // TODO should come from config
        return healthController;
    }

    // Beans without unit tests ////////////////////////////////////////////////////////////////////////////////////////
    @Bean
    SpanSerdeFactory spanSerdeFactory() {
        return new SpanSerdeFactory();
    }

    @Bean
    JsonTransformerIsActiveController.Factory jsonTransformerIsActiveControllerFactory() {
        return new JsonTransformerIsActiveController.Factory();
    }

    @Bean
    @Autowired
    ProtobufToJsonTransformer protobufToJsonTransformer(KafkaStreamStarter kafkaStreamStarter,
                                                        SpanSerdeFactory spanSerdeFactory) {
        return new ProtobufToJsonTransformer(kafkaStreamStarter, spanSerdeFactory);
    }

}
