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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.util.VisibleForTesting;

import org.slf4j.Logger;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.PROTOBUF_ERROR_MSG;

@Component
class Batch {
    @VisibleForTesting
    static final String ERROR_CODES_AND_MESSAGES_OF_FAILURES =
            "Error codes and messages for failures=[%s]; retryCount=%d";
    @VisibleForTesting
    static final String RESULT_NULL = "PutRecordBatchResult is null; retrying %d records; retryCount=%d";
    @VisibleForTesting
    static final String THROTTLED_ERROR_CODE = "ServiceUnavailableException";
    @VisibleForTesting
    static final String THROTTLED_MESSAGE = "Slow down.";

    private final TagFlattener tagFlattener = new TagFlattener();
    private final JsonFormat.Printer printer;
    private final FirehoseCollector firehoseCollector;
    private final Logger logger;
    private final Counter throttledCounter;

    @Autowired
    Batch(JsonFormat.Printer printer,
          Supplier<FirehoseCollector> firehoseCollector,
          Logger batchLogger,
          Counter throttledCounter) {
        this.printer = printer;
        this.firehoseCollector = firehoseCollector.get();
        this.logger = batchLogger;
        this.throttledCounter = throttledCounter;
    }

    List<Record> getRecordList(Span span) {
        try {
            final String jsonWithOpenTracingTags = printer.print(span);
            final String jsonWithFlattenedTags = tagFlattener.flattenTags(jsonWithOpenTracingTags);
            return firehoseCollector.addRecordAndReturnBatch(jsonWithFlattenedTags);
        } catch (InvalidProtocolBufferException exception) {
            // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
            logger.error(String.format(PROTOBUF_ERROR_MSG, span.toString(), exception.getMessage()), exception);
            return Collections.emptyList();
        }
    }

    List<Record> getRecordListForShutdown() {
        return firehoseCollector.createIncompleteBatch();
    }

    List<Record> extractFailedRecords(
            PutRecordBatchRequest request, PutRecordBatchResult result, int retryCount) {
        final List<Record> recordsNeedingRetry;
        if (result != null) {
            final List<PutRecordBatchResponseEntry> batchResponseEntries = result.getRequestResponses();
            final Map<String, String> uniqueErrorCodesAndMessages = new TreeMap<>();
            final int failedPutCount = result.getFailedPutCount();
            recordsNeedingRetry = extractFailedRecordsAndAggregateFailures(
                    request, batchResponseEntries, uniqueErrorCodesAndMessages, failedPutCount);
            final Map<String, String> errorsThatAreNotThrottles = countIfThrottled(uniqueErrorCodesAndMessages);
            logFailures(retryCount, errorsThatAreNotThrottles);
        } else {
            recordsNeedingRetry = request.getRecords();
            logger.error(String.format(RESULT_NULL, request.getRecords().size(), retryCount));
        }
        return recordsNeedingRetry;
    }

    private List<Record> extractFailedRecordsAndAggregateFailures(PutRecordBatchRequest request, List<PutRecordBatchResponseEntry> batchResponseEntries, Map<String, String> uniqueErrorCodesAndMessages, int failedPutCount) {
        List<Record> recordsNeedingRetry;
        recordsNeedingRetry = new ArrayList<>(failedPutCount);
        final int totalNumberOfResponses = batchResponseEntries.size();
        for (int i = 0; i < totalNumberOfResponses; i++) {
            final PutRecordBatchResponseEntry putRecordBatchResponseEntry = batchResponseEntries.get(i);
            final String errorCode = putRecordBatchResponseEntry.getErrorCode();
            if (StringUtils.isNotEmpty(errorCode)) {
                uniqueErrorCodesAndMessages.put(errorCode, putRecordBatchResponseEntry.getErrorMessage());
                final List<Record> records = request.getRecords();
                recordsNeedingRetry.add(records.get(i));
            }
        }
        return recordsNeedingRetry;
    }

    @VisibleForTesting
    Map<String, String> countIfThrottled(Map<String, String> uniqueErrorCodesAndMessages) {
        final Iterator<Map.Entry<String, String>> iterator = uniqueErrorCodesAndMessages.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<String, String> mapEntry = iterator.next();
            final String errorCode = mapEntry.getKey();
            final String message = mapEntry.getValue();
            if (THROTTLED_ERROR_CODE.equals(errorCode) && THROTTLED_MESSAGE.equals(message)) {
                iterator.remove();
                throttledCounter.increment();
                break;
            }
        }
        return uniqueErrorCodesAndMessages;
    }

    @VisibleForTesting
    void logFailures(int retryCount, Map<String, String> uniqueErrorCodesAndMessages) {
        if(!uniqueErrorCodesAndMessages.isEmpty()) {
            final String allErrorCodesAndMessages = StringUtils.join(uniqueErrorCodesAndMessages, ',');
            logger.error(String.format(ERROR_CODES_AND_MESSAGES_OF_FAILURES, allErrorCodesAndMessages, retryCount));
        }
    }
}
