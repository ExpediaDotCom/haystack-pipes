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
package com.expedia.www.haystack.pipes.commons.serialization;

import com.expedia.open.tracing.Span;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Test;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SerdeFactoryTest {
    private static final String APPLICATION = RANDOM.nextLong() + "APPLICATION";

    private SerdeFactory serdeFactory;

    @Before
    public void setUp() {
        serdeFactory = new SerdeFactory();
    }

    @Test
    public void testCreateJsonProtoSpanSerde() {
        final Serde<Span> spanSerde = serdeFactory.createJsonProtoSpanSerde(APPLICATION);

        final SpanProtobufDeserializer deserializer = (SpanProtobufDeserializer) spanSerde.deserializer();
        assertEquals(APPLICATION, deserializer.application);
        final SpanJsonSerializer serializer = (SpanJsonSerializer) spanSerde.serializer();
        assertEquals(APPLICATION, serializer.application);
    }

    @Test
    public void testCreateProtoProtoSpanSerde() {
        final Serde<Span> spanSerde = serdeFactory.createProtoProtoSpanSerde(APPLICATION);

        final SpanProtobufDeserializer deserializer = (SpanProtobufDeserializer) spanSerde.deserializer();
        assertEquals(APPLICATION, deserializer.application);
        final SpanProtobufSerializer serializer = (SpanProtobufSerializer) spanSerde.serializer();
        assertEquals(APPLICATION, serializer.application);
    }

    @Test
    public void testCreateStringSerde() {
        final Serde<String> stringSerde = serdeFactory.createStringSerde();

        assertNotNull(stringSerde);
    }
}
