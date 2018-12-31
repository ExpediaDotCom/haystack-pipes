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

import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.firehoseWriter.FailedRecordExtractor.INTERNAL_FAILURE_MSG;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InternalFailureErrorLoggerTest {
    private static final int RETRY_COUNT = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final int SIZE = RANDOM.nextInt(Byte.MAX_VALUE);
    private static final String REQUEST_ID = RANDOM.nextLong() + "REQUEST_ID";

    @Mock
    private Logger mockLogger;
    @Mock
    private PutRecordBatchRequest mockPutRecordBatchRequest;
    @Mock
    private PutRecordBatchResult mockPutRecordBatchResult;
    @Mock
    private ResponseMetadata mockSdkResponseMetadata;
    @Mock
    private Record mockRecord;

    private List<Record> records;
    private InternalFailureErrorLogger internalFailureErrorLogger;

    @Before
    public void setUp() {
        records = new ArrayList<>(SIZE);
        for(int i = 0 ; i < SIZE ; i++) {
            records.add(mockRecord);
        }
        internalFailureErrorLogger = new InternalFailureErrorLogger(mockLogger);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockPutRecordBatchRequest);
        verifyNoMoreInteractions(mockPutRecordBatchResult);
        verifyNoMoreInteractions(mockSdkResponseMetadata);
        verifyNoMoreInteractions(mockRecord);
    }

    @Test
    public void testLogError() {
        when(mockPutRecordBatchRequest.getRecords()).thenReturn(records);
        when(mockPutRecordBatchResult.getSdkResponseMetadata()).thenReturn(mockSdkResponseMetadata);
        when(mockSdkResponseMetadata.getRequestId()).thenReturn(REQUEST_ID);

        internalFailureErrorLogger.logError(mockPutRecordBatchRequest, mockPutRecordBatchResult, RETRY_COUNT);

        verify(mockPutRecordBatchRequest).getRecords();
        verify(mockPutRecordBatchResult).getSdkResponseMetadata();
        verify(mockSdkResponseMetadata).getRequestId();
        verify(mockLogger).error(String.format(INTERNAL_FAILURE_MSG, SIZE, RETRY_COUNT, REQUEST_ID));
    }

}
