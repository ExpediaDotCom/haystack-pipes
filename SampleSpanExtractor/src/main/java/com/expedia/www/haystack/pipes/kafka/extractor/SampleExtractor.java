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
package com.expedia.www.haystack.pipes.kafka.extractor;


import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.key.extractor.SpanKeyExtractor;

import java.util.ArrayList;
import java.util.List;

public class SampleExtractor implements SpanKeyExtractor {


    @Override
    public String name() {
        return "SampleExtractor";
    }

    @Override
    public void configure(String config) {
        //Do nothing
    }

    @Override
    public String extract(Span span) {
        return span.toString();
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public List<String> getTopics() {
        return new ArrayList<>();
    }
}
