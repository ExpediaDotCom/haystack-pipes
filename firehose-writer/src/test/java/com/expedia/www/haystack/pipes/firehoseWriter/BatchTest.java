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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.monitor.Counter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.PROTOBUF_ERROR_MSG;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EXCEPTION_MESSAGE;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_NO_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.Batch.ERROR_CODES_AND_MESSAGES_OF_FAILURES;
import static com.expedia.www.haystack.pipes.firehoseWriter.Batch.INTERNAL_FAILURE_ERROR_CODE;
import static com.expedia.www.haystack.pipes.firehoseWriter.Batch.INTERNAL_FAILURE_MSG;
import static com.expedia.www.haystack.pipes.firehoseWriter.Batch.RESULT_NULL;
import static com.expedia.www.haystack.pipes.firehoseWriter.Batch.THROTTLED_ERROR_CODE;
import static com.expedia.www.haystack.pipes.firehoseWriter.Batch.THROTTLED_MESSAGE;
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
    @Mock
    private Map<String, String> mockMap;
    @Mock
    private Iterator<Map.Entry<String, String>> mockIterator;
    @Mock
    private Set<Map.Entry<String, String>> mockEntrySet;
    @Mock
    private Map.Entry<String, String> mockMapEntry;
    @Mock
    private Counter mockCounter;

    private Batch batch;

    @Before
    public void setUp() {
        final JsonFormat.Printer realPrinter = JsonFormat.printer().omittingInsignificantWhitespace();
        batch = new Batch(realPrinter, () -> mockFirehoseCollector, mockLogger, mockCounter);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockPrinter, mockFirehoseCollector, mockLogger, mockCounter);
        verifyNoMoreInteractions(mockRequest, mockResult, mockRecord, mockPutRecordBatchResponseEntryList,
                mockPutRecordBatchResponseEntry, mockRecordList, mockMap, mockIterator, mockEntrySet, mockMapEntry);
    }

    @Test
    public void testGetRecordListInvalidProtocolBufferException() throws InvalidProtocolBufferException {
        batch = new Batch(mockPrinter, () -> mockFirehoseCollector, mockLogger, mockCounter);
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testExtractFailedRecordsNonNullResultNotInternalFailure() {
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
        when(mockPutRecordBatchResponseEntry.getRecordId()).thenReturn("A_RecordId").thenReturn("B_RecordId");
        when(mockRequest.getRecords()).thenReturn(mockRecordList);
        when(mockRecordList.get(anyInt())).thenReturn(mockRecord);

        batch.extractFailedRecords(mockRequest, mockResult, RETRY_COUNT);

        verify(mockResult).getRequestResponses();
        verify(mockResult).getFailedPutCount();
        verify(mockPutRecordBatchResponseEntryList).size();
        for (int i = 0; i < totalCount; i++) {
            //noinspection ResultOfMethodCallIgnored
            verify(mockPutRecordBatchResponseEntryList).get(i);
        }
        verify(mockPutRecordBatchResponseEntry, times(totalCount)).getErrorCode();
        verify(mockPutRecordBatchResponseEntry, times(failureCount)).getErrorMessage();
        verify(mockPutRecordBatchResponseEntry, times(failureCount)).getRecordId();
        verify(mockRequest, times(3)).getRecords();
        verify(mockRecordList).get(1);
        verify(mockRecordList).get(3);
        verify(mockRecordList).get(4);
        final String uniqueErrors = "{A_once=Error Message: [A_message] Record ID: [A_RecordId], B_twice=Error Message: [B_message] Record ID: [B_RecordId]},";
        verify(mockLogger).warn(String.format(ERROR_CODES_AND_MESSAGES_OF_FAILURES, uniqueErrors, RETRY_COUNT));
    }


    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testExtractFailedRecordsNonNullResultInternalFailure() {
        when(mockResult.getRequestResponses()).thenReturn(mockPutRecordBatchResponseEntryList);
        when(mockResult.getFailedPutCount()).thenReturn(1, 0);
        when(mockPutRecordBatchResponseEntryList.size()).thenReturn(1);
        when(mockPutRecordBatchResponseEntryList.get(anyInt())).thenReturn(mockPutRecordBatchResponseEntry);
        when(mockPutRecordBatchResponseEntry.getErrorCode()).thenReturn(INTERNAL_FAILURE_ERROR_CODE);
        when(mockPutRecordBatchResponseEntry.getErrorMessage()).thenReturn("Internal Server Error");
        when(mockPutRecordBatchResponseEntry.getRecordId()).thenReturn("RecordId");
        when(mockRequest.getRecords()).thenReturn(Collections.singletonList(mockRecord));

        batch.extractFailedRecords(mockRequest, mockResult, RETRY_COUNT);

        verify(mockResult).getRequestResponses();
        verify(mockResult).getFailedPutCount();
        verify(mockPutRecordBatchResponseEntryList).size();
        //noinspection ResultOfMethodCallIgnored
        verify(mockPutRecordBatchResponseEntryList).get(0);
        verify(mockPutRecordBatchResponseEntry).getErrorCode();
        verify(mockPutRecordBatchResponseEntry).getErrorMessage();
        verify(mockPutRecordBatchResponseEntry).getRecordId();
        verify(mockRequest, times(2)).getRecords();
        verify(mockLogger).error(String.format(INTERNAL_FAILURE_MSG, 1));
        verify(mockLogger).warn(String.format(ERROR_CODES_AND_MESSAGES_OF_FAILURES,
                "{InternalFailure=Error Message: [Internal Server Error] Record ID: [RecordId]},", RETRY_COUNT));
    }

    @Test
    public void testExtractFailedRecordsNullResult() {
        when(mockRequest.getRecords()).thenReturn(mockRecordList);
        when(mockRecordList.size()).thenReturn(SIZE);

        batch.extractFailedRecords(mockRequest, null, RETRY_COUNT);

        verify(mockRequest, times(2)).getRecords();
        verify(mockRecordList).size();
        verify(mockLogger).error(String.format(RESULT_NULL, SIZE, RETRY_COUNT));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testCountIfThrottled() {
        // Using these mocks makes the test more verbose but verifies that break; is called appropriately
        when(mockMap.entrySet()).thenReturn(mockEntrySet);
        when(mockEntrySet.iterator()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true);
        when(mockIterator.next()).thenReturn(mockMapEntry);
        when(mockMapEntry.getKey()).thenReturn(THROTTLED_ERROR_CODE, null, THROTTLED_ERROR_CODE);
        when(mockMapEntry.getValue()).thenReturn(null, THROTTLED_MESSAGE, THROTTLED_MESSAGE);

        batch.countThrottled(mockMap);

        verify(mockCounter).increment();
        verify(mockMap).entrySet();
        verify(mockEntrySet).iterator();
        verify(mockIterator, times(3)).hasNext();
        verify(mockIterator, times(3)).next();
        verify(mockMapEntry, times(3)).getKey();
        verify(mockMapEntry, times(3)).getValue();
        verify(mockIterator).remove();
    }

    @Test
    public void testLogFailuresEmptyMap() {
        batch.logFailures(Integer.MAX_VALUE, Collections.emptyMap());
    }
}
