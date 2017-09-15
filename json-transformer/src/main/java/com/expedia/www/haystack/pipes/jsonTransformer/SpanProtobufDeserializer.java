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
package com.expedia.www.haystack.pipes.jsonTransformer;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.jsonTransformer.Constants.APPLICATION;
import static com.expedia.www.haystack.pipes.jsonTransformer.Constants.SUBSYSTEM;

public class SpanProtobufDeserializer implements Deserializer<Span> {
    static final String ERROR_MSG = "Problem deserializing span [%s]";
    static Logger logger = LoggerFactory.getLogger(SpanProtobufDeserializer.class);
    private static final String KLASS_NAME = SpanProtobufDeserializer.class.getSimpleName();
    private static final MetricObjects METRIC_OBJECTS = new MetricObjects();
    static final Counter REQUEST = METRIC_OBJECTS.createAndRegisterCounter(SUBSYSTEM, APPLICATION, KLASS_NAME, "REQUEST");
    static final Counter ERROR = METRIC_OBJECTS.createAndRegisterCounter(SUBSYSTEM, APPLICATION, KLASS_NAME, "ERROR");
    static final Counter BYTES_IN = METRIC_OBJECTS.createAndRegisterCounter(SUBSYSTEM, APPLICATION, KLASS_NAME, "BYTES_IN");
    static Timer PROTOBUF_DESERIALIZATION = METRIC_OBJECTS.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION, KLASS_NAME,
            "PROTOBUF_DESERIALIZATION", TimeUnit.MICROSECONDS);

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // Nothing to do
    }

    @Override
    public Span deserialize(String key, byte[] bytes) {
        REQUEST.increment();
        if (bytes == null) {
            return null;
        }
        final Stopwatch stopwatch = PROTOBUF_DESERIALIZATION.start();
        try {
            BYTES_IN.increment(bytes.length);
            return Span.parseFrom(bytes);
        } catch (Exception exception) {
            ERROR.increment();
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
