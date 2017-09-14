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
package com.expedia.www.haystack.pipes;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.Constants.APPLICATION;
import static com.expedia.www.haystack.pipes.Constants.SUBSYSTEM;

public class SpanJsonSerializer implements Serializer<Span> {
    static final String ERROR_MSG = "Problem serializing span [%s]";
    static Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    static Logger logger = LoggerFactory.getLogger(SpanJsonSerializer.class);
    private static final String KLASS_NAME = SpanJsonSerializer.class.getSimpleName();

    private static final MetricObjects METRIC_OBJECTS = new MetricObjects();
    static final Counter REQUEST = METRIC_OBJECTS.createAndRegisterCounter(SUBSYSTEM, APPLICATION, KLASS_NAME, "REQUEST");
    static final Counter ERROR = METRIC_OBJECTS.createAndRegisterCounter(SUBSYSTEM, APPLICATION, KLASS_NAME, "ERROR");
    static final Counter BYTES_IN = METRIC_OBJECTS.createAndRegisterCounter(SUBSYSTEM, APPLICATION, KLASS_NAME, "BYTES_IN");
    static Timer JSON_SERIALIZATION = METRIC_OBJECTS.createAndRegisterBasicTimer(SUBSYSTEM, KLASS_NAME, APPLICATION,
            "JSON_SERIALIZATION", TimeUnit.MICROSECONDS);

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // Nothing to do
    }

    @Override
    public byte[] serialize(String key, Span span) {
        REQUEST.increment();
        final Stopwatch stopwatch = JSON_SERIALIZATION.start();
        try {
            final byte[] bytes = printer.print(span).getBytes(Charset.forName("UTF-8"));
            BYTES_IN.increment(bytes.length);
            return bytes;
        } catch (Exception exception) {
            ERROR.increment();
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
