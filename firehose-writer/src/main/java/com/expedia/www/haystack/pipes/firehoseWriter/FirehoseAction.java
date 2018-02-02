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

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.PROTOBUF_ERROR_MSG;

@Component
public class FirehoseAction implements ForeachAction<String, Span> {
    private final TagFlattener tagFlattener = new TagFlattener();

    private final JsonFormat.Printer printer;
    private final Logger logger;
    private final Counter requestCounter;
    private final FirehoseCollector firehoseCollector;
    private final AmazonKinesisFirehose amazonKinesisFirehose;

    @Autowired
    FirehoseAction(JsonFormat.Printer printer,
                   Logger firehoseActionLogger,
                   Counter requestCounter,
                   FirehoseCollector firehoseCollector,
                   AmazonKinesisFirehose amazonKinesisFirehose) {
        this.printer = printer;
        this.logger = firehoseActionLogger;
        this.requestCounter = requestCounter;
        this.firehoseCollector = firehoseCollector;
        this.amazonKinesisFirehose = amazonKinesisFirehose;
    }

    @Override
    public void apply(String key, Span span) {
        requestCounter.increment();
        final List<Record> batch = getBatch(span);
        // TODO send batch to Fireshose if it is not empty
    }

    @VisibleForTesting
    List<Record> getBatch(Span span) {
        String jsonWithFlattenedTags;
        final String jsonWithOpenTracingTags;
        try {
            jsonWithOpenTracingTags = printer.print(span);
            jsonWithFlattenedTags = tagFlattener.flattenTags(jsonWithOpenTracingTags);
            final Record record = new Record().withData(ByteBuffer.wrap(jsonWithFlattenedTags.getBytes()));
            return firehoseCollector.addRecordAndReturnBatch(record);
        } catch (InvalidProtocolBufferException exception) {
            // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
            final String message = String.format(PROTOBUF_ERROR_MSG, span.toString(), exception.getMessage());
            logger.error(message, exception);
            return Collections.emptyList();
        }
    }

}
