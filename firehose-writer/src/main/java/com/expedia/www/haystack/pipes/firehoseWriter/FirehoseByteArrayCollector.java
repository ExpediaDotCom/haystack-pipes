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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class FirehoseByteArrayCollector implements FirehoseCollector {
    @VisibleForTesting
    static final int MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR = 4;

    private final Factory factory;
    private final int maxBatchInterval;
    private final int maxRecordsInBatch;
    private final int maxBytesInRecord;

    private final byte[] buffer;
    @VisibleForTesting
    int bufferIndex;
    private List<Record> recordsInBatch;
    private long batchLastCreatedAt;
    private int totalBatchSize;

    FirehoseByteArrayCollector() {
        this(0);
    }

    FirehoseByteArrayCollector(int maxBatchInterval) {
        this(new Factory(), MAX_BYTES_IN_RECORD, MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR, maxBatchInterval);
    }

    FirehoseByteArrayCollector(Factory factory,
                               int maxBytesInRecord,
                               int maxRecordsInBatch,
                               int maxBatchInterval) {
        Validate.notNull(factory);
        this.factory = factory;
        this.maxBatchInterval = maxBatchInterval;
        this.maxBytesInRecord = maxBytesInRecord;
        this.maxRecordsInBatch = maxRecordsInBatch;
        this.bufferIndex = 0;
        buffer = new byte[maxBytesInRecord];
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
        final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxBytesInRecord) {
            throw new IllegalArgumentException("length of data [" + bytes.length +
                                                       "] is greater than max size allowed : " + maxBytesInRecord);
        }

        if (shouldCreateNewRecordDueToRecordSize(bytes)) {
            final Optional<Record> record = createRecordFromBuffer();
            record.ifPresent(recordsInBatch::add);

            final List<Record> returnBatch = createNewBatchIfFull();

            addToRecordBuffer(bytes);
            return returnBatch;
        } else {
            addToRecordBuffer(bytes);
            return Collections.emptyList();
        }
    }

    @Override
    public List<Record> createIncompleteBatch() {
        final Optional<Record> record = createRecordFromBuffer();
        record.ifPresent(recordsInBatch::add);
        final List<Record> returnBatch = recordsInBatch;
        initializeBatch();
        return returnBatch;
    }

    @VisibleForTesting
    int expectedBufferSizeWithData(final byte[] bytes) {
        //add 1 for NEW_LINE
        return (bufferIndex + bytes.length + 1);
    }

    @VisibleForTesting
    boolean batchCreationTimedOut() {
        return (maxBatchInterval > 0) &&
                (factory.currentTimeMillis() - batchLastCreatedAt) > maxBatchInterval;
    }

    @VisibleForTesting
    void initializeBuffer() {
        bufferIndex = 0;
    }

    @VisibleForTesting
    boolean shouldCreateNewRecordDueToRecordSize(byte[] bytes) {
        return expectedBufferSizeWithData(bytes) > maxBytesInRecord;
    }

    @VisibleForTesting
    Optional<Record> createRecordFromBuffer() {
        if (bufferIndex == 0) {
            return Optional.empty();
        }

        //add the current buffer to a record and clear the buffer
        final Record record = new Record().withData(
                ByteBuffer.wrap(Arrays.copyOfRange(buffer, 0, bufferIndex))
                        .duplicate());
        // AWS SDK says "It is recommended to call ByteBuffer.duplicate()"; see
        // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/services/kinesisfirehose/AmazonKinesisFirehoseClient.html
        // That Javadoc promises a change; when that happens, the call to duplicate() may no longer be necessary.

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
    void addToRecordBuffer(final byte[] bytes) {
        System.arraycopy(bytes, 0, buffer, bufferIndex, bytes.length);
        bufferIndex = bufferIndex + bytes.length;
        totalBatchSize += bytes.length;
    }

    private void initializeBatch() {
        recordsInBatch = new ArrayList<>(maxRecordsInBatch);
        batchLastCreatedAt = factory.currentTimeMillis();
        totalBatchSize = 0;
        bufferIndex = 0;
    }
}
