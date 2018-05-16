package com.expedia.www.haystack.pipes.firehoseWriter;

import java.util.List;

import com.amazonaws.services.kinesisfirehose.model.Record;

public interface FirehoseCollector {
    List<Record> addRecordAndReturnBatch(byte[] data);
    List<Record> returnIncompleteBatch();
}