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
package com.expedia.www.haystack.pipes.firehoseWriter;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.PROTOBUF_ERROR_MSG;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EXCEPTION_MESSAGE;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_FLATTENED_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseActionTest {
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final Record RECORD = new Record().withData(
            ByteBuffer.wrap(JSON_SPAN_STRING_WITH_FLATTENED_TAGS.getBytes()));

    @Mock
    private Logger mockLogger;
    @Mock
    private Counter mockCounter;
    @Mock
    private FirehoseCollector mockFirehoseCollector;
    @Mock
    private AmazonKinesisFirehose mockAmazonKinesisFirehose;
    @Mock
    private Printer mockPrinter;

    private FirehoseAction firehoseAction;

    @Before
    public void setUp() {
        final Printer realPrinter = JsonFormat.printer().omittingInsignificantWhitespace();
        firehoseAction = new FirehoseAction(
                realPrinter, mockLogger, mockCounter, mockFirehoseCollector, mockAmazonKinesisFirehose);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockLogger, mockCounter, mockFirehoseCollector, mockAmazonKinesisFirehose);
    }

    @Test
    public void testApply() {
        firehoseAction.apply(KEY, FULLY_POPULATED_SPAN);

        verify(mockFirehoseCollector).addRecordAndReturnBatch(RECORD);
        verify(mockCounter).increment();
    }

    @Test
    public void testGetBatchInvalidProtocolBufferException() throws InvalidProtocolBufferException {
        firehoseAction = new FirehoseAction(
                mockPrinter, mockLogger, mockCounter, mockFirehoseCollector, mockAmazonKinesisFirehose);
        final InvalidProtocolBufferException exception = new InvalidProtocolBufferException(EXCEPTION_MESSAGE);
        when(mockPrinter.print(any(Span.class))).thenThrow(exception);

        final List<Record> batch = firehoseAction.getBatch(NO_TAGS_SPAN);

        assertTrue(batch.isEmpty());
        verify(mockPrinter).print(NO_TAGS_SPAN);
        final String message = String.format(PROTOBUF_ERROR_MSG, NO_TAGS_SPAN.toString(), EXCEPTION_MESSAGE);
        verify(mockLogger).error(message, exception);
    }
}
