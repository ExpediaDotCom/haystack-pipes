package com.expedia.www.haystack.pipes.producer;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CallbackFactory extends BasePooledObjectFactory<ProduceIntoExternalKafkaCallback> {
    private final Logger produceIntoExternalKafkaCallbackLogger;

    @Autowired
    CallbackFactory(Logger produceIntoExternalKafkaCallbackLogger) {
        this.produceIntoExternalKafkaCallbackLogger = produceIntoExternalKafkaCallbackLogger;
    }

    @Override
    public ProduceIntoExternalKafkaCallback create() {
        return new ProduceIntoExternalKafkaCallback(produceIntoExternalKafkaCallbackLogger);
    }

    @Override
    public PooledObject<ProduceIntoExternalKafkaCallback> wrap(
            ProduceIntoExternalKafkaCallback produceIntoExternalKafkaCallback) {
        return new DefaultPooledObject<>(produceIntoExternalKafkaCallback);
    }
}
