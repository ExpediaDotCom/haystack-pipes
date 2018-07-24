package com.expedia.www.haystack.pipes.firehoseWriter;

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.lang3.Validate;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class FirehoseStringBufferCollector implements FirehoseCollector {
    @VisibleForTesting
    static final int MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR = 4;

    private final Factory factory;
    private final int maxBatchInterval;
    private final int maxRecordsInBatch;
    private final int maxBytesInRecord;

    private StringBuilder buffer;
    private List<Record> recordsInBatch;
    private long batchLastCreatedAt;
    private int totalBatchSize;

    FirehoseStringBufferCollector() {
        this(0);
    }

    FirehoseStringBufferCollector(int maxBatchInterval) {
        this(new Factory(), MAX_BYTES_IN_RECORD, MAX_RECORDS_IN_BATCH_FOR_STRING_BUFFER_COLLECTOR, maxBatchInterval);
    }

    FirehoseStringBufferCollector(Factory factory,
                                  int maxBytesInRecord,
                                  int maxRecordsInBatch,
                                  int maxBatchInterval) {
        Validate.notNull(factory);
        this.factory = factory;
        this.maxBatchInterval = maxBatchInterval;
        this.maxBytesInRecord = maxBytesInRecord;
        this.maxRecordsInBatch = maxRecordsInBatch;
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
            final Optional<Record> record = createRecordFromBuffer();
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
    public List<Record> returnIncompleteBatch() {
        final Optional<Record> record = createRecordFromBuffer();
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
    Optional<Record> createRecordFromBuffer() {
        if (bufferSize() == 0) {
            return Optional.empty();
        }

        //add the current buffer to a record and clear the buffer
        final Record record = new Record().withData(ByteBuffer.wrap(buffer.toString().getBytes()));
        initializeBuffer();
        return Optional.of(record);
    }

    @VisibleForTesting
    List<Record> createNewBatchIfFull() {
        //if the current batch is ready to be sent, initialize a new batch
        //otherwise we get an empty batch
        final List<Record> returnBatch;
        if (recordsInBatch.size() == maxRecordsInBatch || batchCreationTimedOut()) {
            final Optional<Record> record = createRecordFromBuffer();
            record.ifPresent(recordsInBatch::add);
            returnBatch = recordsInBatch;
            initializeBatch();
        }
        else {
            returnBatch = Collections.emptyList();
        }
        return returnBatch;
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
