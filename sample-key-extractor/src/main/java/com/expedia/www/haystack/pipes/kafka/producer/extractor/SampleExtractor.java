/*
 * Copyright 2020 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.kafka.producer.extractor;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.key.extractor.Record;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.util.VisibleForTesting;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SampleExtractor implements SpanKeyExtractor {

    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(SampleExtractor.class);
    @VisibleForTesting
    static JsonFormat.Printer jsonPrinter = JsonFormat.printer().omittingInsignificantWhitespace();
    private final String key = "sampleKafkaKey";
    private List<String> producers;
    private Map<String, List<String>> producerTopicsMapping;

    @Override
    public String name() {
        return "SampleExtractor";
    }

    @Override
    public void configure(Config config) {
        logger.info("{} class loaded with config: {}", SampleExtractor.class.getSimpleName(), config);
        List<Config> producerConf = (List<Config>) config.getConfigList("producers");
        // populating producer topic mapping from configuration
        producerTopicsMapping = producerConf.stream().collect(Collectors.toMap(conf -> conf.getString("name"),
                conf -> conf.getConfig("config").getStringList("topics")));
    }

    @Override
    public List<Record> getRecords(Span span) {
        try {
            Record record = new Record(jsonPrinter.print(span), key, producerTopicsMapping);
            return Collections.singletonList(record);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Exception occurred while extracting span with traceId:{} {}", span.getTraceId(), e.getMessage());
        }
        return Collections.emptyList();
    }

}
