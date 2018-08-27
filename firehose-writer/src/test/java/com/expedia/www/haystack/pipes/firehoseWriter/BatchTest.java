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

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.PROTOBUF_ERROR_MSG;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EXCEPTION_MESSAGE;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_NO_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BatchTest {
    @Mock
    private JsonFormat.Printer mockPrinter;
    @Mock
    private FirehoseCollector mockFirehoseCollector;
    @Mock
    private Logger mockLogger;
    @Mock
    private List<Record> mockRecordList;

    private Batch batch;

    @Before
    public void setUp() {
        final JsonFormat.Printer realPrinter = JsonFormat.printer().omittingInsignificantWhitespace();
        batch = new Batch(realPrinter, () -> mockFirehoseCollector, mockLogger);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockPrinter);
        verifyNoMoreInteractions(mockFirehoseCollector);
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockRecordList);
    }

    @Test
    public void testGetRecordListInvalidProtocolBufferException() throws InvalidProtocolBufferException {
        batch = new Batch(mockPrinter, () -> mockFirehoseCollector, mockLogger);
        final InvalidProtocolBufferException exception = new InvalidProtocolBufferException(EXCEPTION_MESSAGE);
        when(mockPrinter.print(any(Span.class))).thenThrow(exception);

        final List<Record> recordList = batch.getRecordList(NO_TAGS_SPAN);

        assertTrue(recordList.isEmpty());
        verify(mockPrinter).print(NO_TAGS_SPAN);
        final String message = String.format(PROTOBUF_ERROR_MSG, NO_TAGS_SPAN.toString(), EXCEPTION_MESSAGE);
        verify(mockLogger).error(message, exception);
    }

    @Test
    public void testGetRecordListHappyPath() {
        when(mockFirehoseCollector.addRecordAndReturnBatch(any())).thenReturn(Collections.emptyList());

        final List<Record> recordList = batch.getRecordList(NO_TAGS_SPAN);

        assertSame(Collections.<Record>emptyList(), recordList);
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockFirehoseCollector).addRecordAndReturnBatch(argumentCaptor.capture());
        assertArrayEquals(JSON_SPAN_STRING_WITH_NO_TAGS.getBytes(), argumentCaptor.getValue().getBytes());
    }

    @Test
    public void testGetRecordListForShutdown() {
        when(mockFirehoseCollector.createIncompleteBatch()).thenReturn(mockRecordList);

        assertSame(mockRecordList, batch.getRecordListForShutdown());

        verify(mockFirehoseCollector).createIncompleteBatch();
    }

}
