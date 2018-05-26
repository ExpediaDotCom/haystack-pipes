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
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpanProtobufSerializer extends SerializerDeserializerBase implements Serializer<Span> {
    static final String PROTOBUF_SERIALIZATION_TIMER_NAME = "PROTOBUF_SERIALIZATION";
    static Logger logger = LoggerFactory.getLogger(SpanProtobufSerializer.class);

    static final Map<String, Timer> PROTOBUF_SERIALIZATION_TIMERS = new ConcurrentHashMap<>();

    private final Timer protobufSerialization;

    @SuppressWarnings("WeakerAccess")
    public SpanProtobufSerializer(String application) {
        super(application);
        protobufSerialization = getOrCreateTimer(PROTOBUF_SERIALIZATION_TIMERS, PROTOBUF_SERIALIZATION_TIMER_NAME);
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // Nothing to do
    }

    @Override
    public byte[] serialize(String key, Span span) {
        request.increment();
        final Stopwatch stopwatch = protobufSerialization.start();
        final byte[] bytes = span.toByteArray();
        bytesIn.increment(bytes.length);
        stopwatch.stop();
        return bytes;
    }

    @Override
    public void close() {
        // Nothing to do
    }

}
