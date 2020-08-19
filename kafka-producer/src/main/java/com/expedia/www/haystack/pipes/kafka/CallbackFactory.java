package com.expedia.www.haystack.pipes.kafka;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
