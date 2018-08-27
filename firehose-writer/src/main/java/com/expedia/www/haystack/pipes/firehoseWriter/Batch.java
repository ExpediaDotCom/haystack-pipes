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
import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.PROTOBUF_ERROR_MSG;

@Component
class Batch {
    private final TagFlattener tagFlattener = new TagFlattener();
    private final JsonFormat.Printer printer;
    private final FirehoseCollector firehoseCollector;
    private final Logger logger;

    @Autowired
    Batch(JsonFormat.Printer printer,
          Supplier<FirehoseCollector> firehoseCollector,
          Logger batchLogger) {
        this.printer = printer;
        this.firehoseCollector = firehoseCollector.get();
        this.logger = batchLogger;
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

}
