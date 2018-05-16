package com.expedia.www.haystack.pipes.firehoseWriter;

import java.util.List;

import com.amazonaws.services.kinesisfirehose.model.Record;

/**
 * Interface for implementations that buffer data to be written
 * to firehose
 */
public interface FirehoseCollector {
    /**
     * Maximum record size in bytes; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to read that "The
     * PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is smaller."
     */
    int MAX_BYTES_IN_BATCH = 4 * 1024 * 1024; // = 4 MB
    /**
     * Maximum number of Records allowed in a batch; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to
     * read that "The PutRecordBatch operation can take up to 500 records per call or 4 MB per call, whichever is
     * smaller."
     */
    int MAX_RECORDS_IN_BATCH = 500;

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
    List<Record> addRecordAndReturnBatch(byte[] data);

    /**
     * This method return the remaining data if any that has been buffered as a collection of
     * Records to be dispatched to firehose
     * @return collection of records or an empty collection
     */
    List<Record> returnIncompleteBatch();
}