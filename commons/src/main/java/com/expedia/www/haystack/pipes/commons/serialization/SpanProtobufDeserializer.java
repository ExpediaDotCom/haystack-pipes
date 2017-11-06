/*
 * Copyright 2017 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.commons.serialization;

import com.expedia.open.tracing.Span;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpanProtobufDeserializer extends SerializerDeserializerBase implements Deserializer<Span> {
    static final String ERROR_MSG = "Problem deserializing span [%s]";
    static final String PROTOBUF_SERIALIZATION_TIMER_NAME = "PROTOBUF_DESERIALIZATION";
    static Logger logger = LoggerFactory.getLogger(SpanProtobufDeserializer.class);
    static final Map<String, Timer> PROTOBUF_SERIALIZATION_TIMERS = new ConcurrentHashMap<>();

    private final Timer protobufSerializationTimer;

    public SpanProtobufDeserializer(String application) {
        super(application);
        synchronized (this.application) {
            protobufSerializationTimer = getOrCreateTimer(PROTOBUF_SERIALIZATION_TIMERS, PROTOBUF_SERIALIZATION_TIMER_NAME);
        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // Nothing to do
    }

    @Override
    public Span deserialize(String key, byte[] bytes) {
        request.increment();
        if (bytes == null) {
            return null;
        }
        final Stopwatch stopwatch = protobufSerializationTimer.start();
        try {
            bytesIn.increment(bytes.length);
            return Span.parseFrom(bytes);
        } catch (Exception exception) {
            logger.error(ERROR_MSG, DatatypeConverter.printHexBinary(bytes), exception);
        } finally {
            stopwatch.stop();
        }
        return null;
    }

    @Override
    public void close() {
        // Nothing to do
    }
}
