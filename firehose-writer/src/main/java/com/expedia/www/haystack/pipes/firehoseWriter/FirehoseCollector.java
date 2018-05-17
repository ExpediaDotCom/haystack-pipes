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

import java.util.List;

import com.amazonaws.services.kinesisfirehose.model.Record;

/**
 * Interface for implementations that buffer data to be written
 * to firehose
 */
public interface FirehoseCollector {
    /**
     * Maximum batch size in bytes; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to read that "The
     * PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller."
     */
    int MAX_BYTES_IN_BATCH = 4 * 1000 * 1000; // ~ 4 MB

    /**
     * Maximum record size in bytes; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to read that "The
     * maximum size of a record sent to Kinesis Data Firehose, before base64-encoding, is 1,000 KB."
     */
    int MAX_BYTES_IN_RECORD = 1000 * 1000; // ~ 1 MB

    /**
     * Maximum number of Records allowed in a batch; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to
     * read that "The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is
     * smaller."
     */
    int MAX_RECORDS_IN_BATCH = 500;

    /**
     * Max time allowed for buffering
     */
    long LAST_BATCH_TIME_DIFF_ALLOWED_MILLIS = 3000;

    /**
     * this method should buffer the given data and return a collection
     * of records that are ready to be dispatched as one batch to firehose
     *
     * this method should make sure the number of records do not exceed
     * {@link #MAX_RECORDS_IN_BATCH} or the total size of the batch do not
     * exceed {@link #MAX_BYTES_IN_BATCH}
     * @param data data to be appended to the next batch
     * @return full batch that is ready to be dispatched
     */
    List<Record> addRecordAndReturnBatch(String data);

    /**
     * This method return the remaining data if any that has been buffered as a collection of
     * Records to be dispatched to firehose
     * @return collection of records or an empty collection
     */
    List<Record> returnIncompleteBatch();

    class Factory {
        long currentTimeMillis() {
            return System.currentTimeMillis();
        }
    }
}