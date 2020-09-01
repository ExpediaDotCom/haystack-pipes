package com.expedia.www.haystack.pipes.producer;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class CallbackFactory extends BasePooledObjectFactory<KafkaCallback> {


    @Override
    public KafkaCallback create() {
        return new KafkaCallback();
    }

    @Override
    public PooledObject<KafkaCallback> wrap(
            KafkaCallback produceIntoExternalKafkaCallback) {
        return new DefaultPooledObject<>(produceIntoExternalKafkaCallback);
    }
}
