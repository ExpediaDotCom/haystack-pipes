package com.expedia.www.haystack.pipes.kafkaproducer;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CallbackFactory extends BasePooledObjectFactory<KafkaToExternalKafkaCallback> {
    private final Logger produceIntoExternalKafkaCallbackLogger;

    @Autowired
    CallbackFactory(Logger produceIntoExternalKafkaCallbackLogger) {
        this.produceIntoExternalKafkaCallbackLogger = produceIntoExternalKafkaCallbackLogger;
    }

    @Override
    public KafkaToExternalKafkaCallback create() {
        return new KafkaToExternalKafkaCallback(produceIntoExternalKafkaCallbackLogger);
    }

    @Override
    public PooledObject<KafkaToExternalKafkaCallback> wrap(
            KafkaToExternalKafkaCallback produceIntoExternalKafkaCallback) {
        return new DefaultPooledObject<>(produceIntoExternalKafkaCallback);
    }
}
