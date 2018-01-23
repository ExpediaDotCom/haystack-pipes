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
package com.expedia.www.haystack.pipes.firehoseWriter;

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaConfigurationProvider;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.util.VisibleForTesting;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.firehoseWriter.Constants.APPLICATION;
import static com.expedia.www.haystack.pipes.firehoseWriter.ProtobufToFirehoseProducer.CLIENT_ID;

@Configuration
@ComponentScan(basePackageClasses = SpringConfig.class)
public class SpringConfig {
    @VisibleForTesting
    static MetricObjects metricObjects = new MetricObjects();

    @Bean
    public Counter requestCounter() {
        return metricObjects.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION,
                FirehoseAction.class.getName(), "REQUEST");
    }

    @Bean
    public KafkaStreamStarter kafkaStreamStarter() {
        return new KafkaStreamStarter(ProtobufToFirehoseProducer.class, CLIENT_ID);
    }

    @Bean
    public SpanSerdeFactory spanSerdeFactory() {
        return new SpanSerdeFactory();
    }

    @Bean
    public FirehoseIsActiveController.Factory firehoseIsActiveControllerFactory() {
        return new FirehoseIsActiveController.Factory();
    }

    @Bean
    public FirehoseAction firehoseAction() {
        return new FirehoseAction(requestCounter());
    }

    @Bean
    public KafkaConfigurationProvider kafkaConfigurationProvider() {
        return new KafkaConfigurationProvider();
    }

    @Bean
    public ProtobufToFirehoseProducer protobufToFirehoseProducer() {
        return new ProtobufToFirehoseProducer(
                kafkaStreamStarter(), spanSerdeFactory(), firehoseAction(), kafkaConfigurationProvider());
    }

}
