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

import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.PROTOBUF_ERROR_MSG;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EXCEPTION_MESSAGE;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_NO_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.Batch.ERROR_CODES_AND_MESSAGES_OF_FAILURES;
import static com.expedia.www.haystack.pipes.firehoseWriter.Batch.RESULT_NULL;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BatchTest {
    private static final int RETRY_COUNT = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final int SIZE = 1 + RANDOM.nextInt(Byte.MAX_VALUE);

    @Mock
    private JsonFormat.Printer mockPrinter;
    @Mock
    private FirehoseCollector mockFirehoseCollector;
    @Mock
    private Logger mockLogger;
    @Mock
    private PutRecordBatchRequest mockRequest;
    @Mock
    private PutRecordBatchResult mockResult;
    @Mock
    private Record mockRecord;
    @Mock
    private List<PutRecordBatchResponseEntry> mockPutRecordBatchResponseEntryList;
    @Mock
    private PutRecordBatchResponseEntry mockPutRecordBatchResponseEntry;
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
        verifyNoMoreInteractions(mockPrinter, mockFirehoseCollector, mockLogger);
        verifyNoMoreInteractions(mockRequest, mockResult, mockRecord, mockPutRecordBatchResponseEntryList,
                mockPutRecordBatchResponseEntry, mockRecordList);
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
        when(mockFirehoseCollector.addRecordAndReturnBatch(any(Record.class))).thenReturn(Collections.emptyList());

        final List<Record> recordList = batch.getRecordList(NO_TAGS_SPAN);

        assertSame(Collections.<Record>emptyList(), recordList);
        ArgumentCaptor<Record> argumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockFirehoseCollector).addRecordAndReturnBatch(argumentCaptor.capture());
        final Record record = argumentCaptor.getValue();
        final ByteBuffer byteBuffer = record.getData();
        assertArrayEquals(JSON_SPAN_STRING_WITH_NO_TAGS.getBytes(), byteBuffer.array());
    }

    @Test
    public void testGetRecordListForShutdown() {
        when(mockFirehoseCollector.returnIncompleteBatch()).thenReturn(mockRecordList);

        assertSame(mockRecordList, batch.getRecordListForShutdown());

        verify(mockFirehoseCollector).returnIncompleteBatch();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testExtractFailedRecordsNonNullResult() {
        final int failureCount = 3;
        final int successCount = 2;
        final int totalCount = failureCount + successCount;
        when(mockResult.getRequestResponses()).thenReturn(mockPutRecordBatchResponseEntryList);
        when(mockResult.getFailedPutCount()).thenReturn(failureCount);  // 2 codes among the 3 failures
        when(mockPutRecordBatchResponseEntryList.size()).thenReturn(totalCount);
        when(mockPutRecordBatchResponseEntryList.get(anyInt())).thenReturn(mockPutRecordBatchResponseEntry);
        when(mockPutRecordBatchResponseEntry.getErrorCode())
                .thenReturn("").thenReturn("A_once").thenReturn(null).thenReturn("B_twice");
        when(mockPutRecordBatchResponseEntry.getErrorMessage()).thenReturn("A_message").thenReturn("B_message");
        when(mockRequest.getRecords()).thenReturn(mockRecordList);
        when(mockRecordList.get(anyInt())).thenReturn(mockRecord);

        batch.extractFailedRecords(mockRequest, mockResult, RETRY_COUNT);

        verify(mockResult).getRequestResponses();
        verify(mockResult).getFailedPutCount();
        verify(mockPutRecordBatchResponseEntryList).size();
        for(int i = 0 ; i < totalCount ; i++) {
            //noinspection ResultOfMethodCallIgnored
            verify(mockPutRecordBatchResponseEntryList).get(i);
        }
        verify(mockPutRecordBatchResponseEntry, times(totalCount)).getErrorCode();
        verify(mockPutRecordBatchResponseEntry, times(failureCount)).getErrorMessage();
        verify(mockRequest, times(3)).getRecords();
        verify(mockRecordList).get(1);
        verify(mockRecordList).get(3);
        verify(mockRecordList).get(4);
        final String uniqueErrors = "{A_once=A_message, B_twice=B_message},";
        verify(mockLogger).warn(String.format(ERROR_CODES_AND_MESSAGES_OF_FAILURES, uniqueErrors, RETRY_COUNT));
    }

    @Test
    public void testExtractFailedRecordsNullResult() {
        when(mockRequest.getRecords()).thenReturn(mockRecordList);
        when(mockRecordList.size()).thenReturn(SIZE);

        batch.extractFailedRecords(mockRequest, null, RETRY_COUNT);

        verify(mockRequest, times(2)).getRecords();
        verify(mockRecordList).size();
        verify(mockLogger).warn(String.format(RESULT_NULL, SIZE, RETRY_COUNT));
    }
}
