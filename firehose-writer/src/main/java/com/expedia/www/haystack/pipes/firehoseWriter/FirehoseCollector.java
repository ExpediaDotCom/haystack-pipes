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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class collects records to send to Firehose until either the maximum number of records, or the maximum total size
 * of the payload (both of which are specified by Firehose documents) is reached.
 */
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

    /**
     * maximum time allowed since last batch was created
     */
    private static final long LAST_BATCH_TIME_DIFF_ALLOWED = 3000;

    private int totalDataSizeOfRecords;
    private List<Record> records;
    private long batchLastCreatedAt;

    FirehoseCollector() {
        initialize();
    }

    private void initialize() {
        records = new ArrayList<>(MAX_RECORDS_IN_BATCH);
        totalDataSizeOfRecords = 0;
        batchLastCreatedAt = System.currentTimeMillis();
    }

    private boolean shouldCreateNewBatchDueToRecordCount() {
        return records.size() == MAX_RECORDS_IN_BATCH;
    }

    private boolean shouldCreateNewBatchDueToDataSize(Record record) {
        return (totalDataSizeOfRecords + record.getData().array().length) > MAX_BYTES_IN_BATCH;
    }

    private boolean batchCreationTimedout() {
        return (System.currentTimeMillis() - batchLastCreatedAt) > LAST_BATCH_TIME_DIFF_ALLOWED;
    }

    @VisibleForTesting
    boolean shouldCreateNewBatch(Record record) {
        return records.size() > 0 &&
                (shouldCreateNewBatchDueToDataSize(record) || shouldCreateNewBatchDueToRecordCount() || batchCreationTimedout());
    }

    @VisibleForTesting
    List<Record> addRecordAndReturnBatch(Record record) {
        if (shouldCreateNewBatch(record)) {
            final List<Record> records = this.records;
            initialize();
            addRecord(record);
            return records;
        } else {
            addRecord(record);
            return Collections.emptyList();
        }
    }

    @VisibleForTesting
    List<Record> returnIncompleteBatch() {
        final List<Record> records = this.records;
        initialize();
        return records;
    }

    private void addRecord(Record record) {
        records.add(record);
        totalDataSizeOfRecords += record.getData().array().length;
    }
}
