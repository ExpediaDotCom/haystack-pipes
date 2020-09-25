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
package com.expedia.www.haystack.pipes.kafka.producer.key.extractor;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.key.extractor.Record;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.util.VisibleForTesting;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JsonExtractor implements SpanKeyExtractor {

    @VisibleForTesting
    static Logger logger = LoggerFactory.getLogger(JsonExtractor.class);
    @VisibleForTesting
    static JsonFormat.Printer jsonPrinter = JsonFormat.printer().omittingInsignificantWhitespace();

    private List<String> producers;

    @Override
    public String name() {
        return "JsonExtractor";
    }

    @Override
    public void configure(Config config) {
        logger.info("{} class loaded with config: {}", JsonExtractor.class.getSimpleName(), config);
        List<Config> producerConf = (List<Config>)config.getConfigList("producers");
        producers = new ArrayList<>();
        producerConf.forEach(conf->producers.add(conf.getString("name")));

    }

    @Override
    public List<Record> getRecords(Span span) {
        try {
            Map<String, List<String>> producerTopicsMapping = new HashMap<>();
            producers.forEach(producer -> {
                producerTopicsMapping.put(producer, Arrays.asList("extractedTopic"));
            });
            Record record = new Record(jsonPrinter.print(span), "externalKafkaKey",
                    producerTopicsMapping);
            return Arrays.asList(record);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Exception occurred while extracting span: " + e.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public List<String> getProducers() {
        return producers;
    }
}
