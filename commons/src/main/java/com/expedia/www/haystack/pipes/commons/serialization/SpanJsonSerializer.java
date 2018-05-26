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
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpanJsonSerializer extends SerializerDeserializerBase implements Serializer<Span> {
    static final String ERROR_MSG = "Problem serializing span into JSON [%s]";
    static final String JSON_SERIALIZATION_TIMER_NAME = "JSON_SERIALIZATION";
    static Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    static Logger logger = LoggerFactory.getLogger(SpanJsonSerializer.class);

    static final Map<String, Timer> JSON_SERIALIZATION_TIMERS = new ConcurrentHashMap<>();

    private final Timer jsonSerialization;

    @SuppressWarnings("WeakerAccess")
    public SpanJsonSerializer(String application) {
        super(application);
        jsonSerialization = getOrCreateTimer(JSON_SERIALIZATION_TIMERS, JSON_SERIALIZATION_TIMER_NAME);
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // Nothing to do
    }

    @Override
    public byte[] serialize(String key, Span span) {
        request.increment();
        final Stopwatch stopwatch = jsonSerialization.start();
        try {
            final byte[] bytes = printer.print(span).getBytes(Charset.forName("UTF-8"));
            bytesIn.increment(bytes.length);
            return bytes;
        } catch (Exception exception) {
            logger.error(ERROR_MSG, span, exception);
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
