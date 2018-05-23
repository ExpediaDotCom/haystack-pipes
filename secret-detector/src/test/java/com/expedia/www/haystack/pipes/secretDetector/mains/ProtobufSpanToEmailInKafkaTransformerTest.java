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
package com.expedia.www.haystack.pipes.secretDetector.mains;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.commons.secretDetector.span.SpanDetector;
import com.expedia.www.haystack.pipes.commons.kafka.KafkaStreamStarter;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProtobufSpanToEmailInKafkaTransformerTest {
    private static final String SECRET = RANDOM.nextLong() + "SECRET";
    private static final Iterable<String> LIST_OF_SECRETS = Collections.singletonList(SECRET);
    @Mock
    private KafkaStreamStarter mockKafkaStreamStarter;
    @Mock
    private KStreamBuilder mockKStreamBuilder;
    @Mock
    private KStream<String, Span> mockKStreamStringSpan;
    @Mock
    private KStream<String, String> mockKStreamStringString;
    @Mock
    private SpanDetector mockSpanDetector;

    private ProtobufSpanToEmailInKafkaTransformer protobufSpanToEmailInKafkaTransformer;

    @Before
    public void setUp() {
        protobufSpanToEmailInKafkaTransformer = new ProtobufSpanToEmailInKafkaTransformer(
                mockKafkaStreamStarter, new SpanSerdeFactory(), mockSpanDetector);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockKafkaStreamStarter, mockKStreamBuilder, mockKStreamStringSpan,
                mockKStreamStringString, mockSpanDetector);
    }

    @Test
    public void testMain() {
        protobufSpanToEmailInKafkaTransformer.main();

        verify(mockKafkaStreamStarter).createAndStartStream(protobufSpanToEmailInKafkaTransformer);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBuildStreamTopology() {
        when(mockKStreamBuilder.stream(Matchers.<Serde<String>>any(), Matchers.<Serde<Span>>any(), anyString()))
                .thenReturn(mockKStreamStringSpan);
        when(mockKStreamStringSpan.flatMapValues(Matchers.<ValueMapper<Span, List<String>>>any()))
                .thenReturn(mockKStreamStringString);
        when(mockSpanDetector.apply(any(Span.class))).thenReturn(LIST_OF_SECRETS);

        protobufSpanToEmailInKafkaTransformer.buildStreamTopology(mockKStreamBuilder);

        verify(mockKStreamBuilder).stream(any(), Matchers.<Serde<Span>>any(), eq("proto-spans"));
        ArgumentCaptor<ValueMapper> argumentCaptor = ArgumentCaptor.forClass(ValueMapper.class);
        verify(mockKStreamStringSpan).flatMapValues(argumentCaptor.capture());
        final ValueMapper<Span, Iterable<String>> valueMapper = argumentCaptor.getValue();
        final Iterable<String> apply = valueMapper.apply(EMAIL_ADDRESS_SPAN);
        final Iterator<String> iterator = apply.iterator();
        assertSame(SECRET, iterator.next());
        verify(mockSpanDetector).apply(EMAIL_ADDRESS_SPAN);
        verify(mockKStreamStringString).to(any(), any(), eq("proto-spans-scrubbed"));
    }
}
