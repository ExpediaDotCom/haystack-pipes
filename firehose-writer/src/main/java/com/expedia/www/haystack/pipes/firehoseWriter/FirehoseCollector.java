package com.expedia.www.haystack.pipes.firehoseWriter;

import com.amazonaws.services.kinesisfirehose.model.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class FirehoseCollector {
    /**
     * Maximum record size in bytes; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to read that "The
     * PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller."
     */
    static final int MAX_BYTES_IN_BATCH = 4 * 1024 * 1024; // = 4 MB
    /**
     * Maximum number of Records allowed in a batch; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to
     * read that "The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is
     * smaller."
     */
    static final int MAX_RECORDS_IN_BATCH = 500;

    private int totalDataSizeOfRecords;
    private List<Record> records;

    FirehoseCollector() {
        initialize();
    }

    private void initialize() {
        records = new ArrayList<>();
        totalDataSizeOfRecords = 0;
    }

    boolean shouldCreateNewBatchDueToRecordCount() {
        return (records.size() + 1) == MAX_RECORDS_IN_BATCH;
    }

    boolean shouldCreateNewBatchDueToDataSize(Record record) {
        return (totalDataSizeOfRecords + record.getData().array().length) > MAX_BYTES_IN_BATCH;
    }

    List<Record> addRecordAndReturnBatch(Record record) {
        final List<Record> records;
        if (shouldCreateNewBatchDueToDataSize(record)) {
            records = this.records;
            initialize();
            addRecord(record);
        } else if (shouldCreateNewBatchDueToRecordCount()) {
            addRecord(record);
            records = this.records;
            initialize();
        } else {
            records = Collections.emptyList();
            addRecord(record);
        }
        return records;
    }

    private void addRecord(Record record) {
        records.add(record);
        totalDataSizeOfRecords += record.getData().array().length;
    }

    List<Record> getRemainingRecords() {
        final List<Record> remainingRecords = records;
        initialize();
        return remainingRecords;
    }
}
