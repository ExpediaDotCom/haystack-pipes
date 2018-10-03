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
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class FirehoseStringBufferCollector implements FirehoseCollector {
    @VisibleForTesting
    static final int MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR = 4;
    static final String BYTE_BUFFER_LENGTH_TOO_LARGE = "Byte buffer length [{}] is too large: {}";

    private final Factory factory;
    private final int maxBatchInterval;
    private final int maxRecordsInBatch;
    private final int maxBytesInRecord;
    private final Logger logger;

    @VisibleForTesting
    StringBuilder buffer; // TODO Replace with "final byte[] bytes" to minimize garbage creation
    private List<Record> recordsInBatch;
    private long batchLastCreatedAt;
    private int totalBatchSize;

    FirehoseStringBufferCollector() {
        this(0);
    }

    FirehoseStringBufferCollector(int maxBatchInterval) {
        this(new Factory(), MAX_BYTES_IN_RECORD, MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR, maxBatchInterval,
                LoggerFactory.getLogger(FirehoseStringBufferCollector.class));
    }

    FirehoseStringBufferCollector(Factory factory,
                                  int maxBytesInRecord,
                                  int maxRecordsInBatch,
                                  int maxBatchInterval,
                                  Logger logger) {
        Validate.notNull(factory);
        this.factory = factory;
        this.maxBatchInterval = maxBatchInterval;
        this.maxBytesInRecord = maxBytesInRecord;
        this.maxRecordsInBatch = maxRecordsInBatch;
        this.logger = logger;
        initializeBuffer();
        initializeBatch();
    }

    int getMaxBatchInterval() {
        return maxBatchInterval;
    }

    int getMaxRecordsInBatch() {
        return maxRecordsInBatch;
    }

    int getMaxBytesInRecord() {
        return maxBytesInRecord;
    }

    int getTotalBatchSize() {
        return totalBatchSize;
    }

    @Override
    public List<Record> addRecordAndReturnBatch(final String data) {
        if (data.length() > maxBytesInRecord) {
            throw new IllegalArgumentException("length of data [" + data.length() +
                                                       "] is greater than max size allowed : " + maxBytesInRecord);
        }

        if (shouldCreateNewRecordDueToRecordSize(data)) {
            final Optional<Record> record = createRecordFromBuffer(buffer);
            record.ifPresent(recordsInBatch::add);

            final List<Record> returnBatch = createNewBatchIfFull();

            addToRecordBuffer(data);
            return returnBatch;
        } else {
            addToRecordBuffer(data);
            return Collections.emptyList();
        }
    }

    @Override
    public List<Record> createIncompleteBatch() {
        final Optional<Record> record = createRecordFromBuffer(buffer);
        record.ifPresent(recordsInBatch::add);
        final List<Record> returnBatch = recordsInBatch;
        initializeBatch();
        return returnBatch;
    }

    @VisibleForTesting
    int expectedBufferSizeWithData(final String data) {
        //add 1 for NEW_LINE
        return (bufferSize() + data.length() + 1);
    }

    @VisibleForTesting
    boolean batchCreationTimedOut() {
        return (maxBatchInterval > 0) &&
                (factory.currentTimeMillis() - batchLastCreatedAt) > maxBatchInterval;
    }

    @VisibleForTesting
    void initializeBuffer() {
        buffer = new StringBuilder();
    }

    @VisibleForTesting
    boolean shouldCreateNewRecordDueToRecordSize(String data) {
        return expectedBufferSizeWithData(data) > maxBytesInRecord;
    }

    @VisibleForTesting
    Optional<Record> createRecordFromBuffer(StringBuilder stringBuilder) {
        if (stringBuilder.length() == 0) {
            return Optional.empty();
        }

        //add the current buffer to a record and clear the buffer
        final byte[] bytes = stringBuilder.toString().getBytes();
        if(bytes.length > MAX_BYTES_IN_RECORD) {
            logger.error(BYTE_BUFFER_LENGTH_TOO_LARGE, bytes.length, Arrays.toString(bytes));
        }
        final Record record = new Record().withData(ByteBuffer.wrap(bytes));
        initializeBuffer();
        return Optional.of(record);
    }

    @VisibleForTesting
    List<Record> createNewBatchIfFull() {
        // If the current batch is ready to be sent, initialize a new batch.
        // If too much time has elapsed since the last batch was sent, send an incomplete batch.
        // Otherwise we get an empty batch.
        if (recordsInBatch.size() == maxRecordsInBatch) {
            final List<Record> recordsInBatchBeforeInitialization = recordsInBatch;
            initializeBatch();
            return recordsInBatchBeforeInitialization;
        } else if(batchCreationTimedOut()) {
            return createIncompleteBatch();
        } else {
            return Collections.emptyList();
        }
    }

    @VisibleForTesting
    void addToRecordBuffer(final String data) {
        buffer.append(data);
        totalBatchSize += data.length();
    }

    @VisibleForTesting
    int bufferSize() {
       return buffer.length();
    }

    private void initializeBatch() {
        recordsInBatch = new ArrayList<>(maxRecordsInBatch);
        batchLastCreatedAt = factory.currentTimeMillis();
        totalBatchSize = 0;
    }
}
