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
package com.expedia.www.haystack.pipes.kafkaProducer.extractor;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SampleExtractor implements SpanKeyExtractor {

    private final JsonFormat.Printer jsonPrinter = JsonFormat.printer();
    private static final Logger logger = LoggerFactory.getLogger("SampleExtractor");

    @Override
    public String name() {
        return "SampleExtractor";
    }

    @Override
    public void configure(Config config) {
        logger.debug("{} got config: {}", name(), config);
    }

    @Override
    public Optional<String> extract(Span span) {
        try {
            return Optional.of(jsonPrinter.print(span));
        } catch (InvalidProtocolBufferException e) {
            logger.error("Exception occurred while extracting span: " + e.getMessage());
        }
        return Optional.empty();
    }

    @Override
    public String getKey() {
        return "dummy-key";
    }

    @Override
    public List<String> getTopics() {
        return new ArrayList<>();
    }

}
