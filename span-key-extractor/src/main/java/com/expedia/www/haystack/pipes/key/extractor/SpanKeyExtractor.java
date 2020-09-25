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
package com.expedia.www.haystack.pipes.key.extractor;

import com.expedia.open.tracing.Span;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Optional;

public interface SpanKeyExtractor {

    String name();

    public void configure(Config config); // sets up the extractor with configuration

    public List<Record> getRecords(Span span); // returns list of records containing message, key and producer topic mapping

    public List<String> getProducers();

}
