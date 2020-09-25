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

import java.util.List;
import java.util.Map;

public class Record {

    String message; // message after span-key extractor manipulation
    String key; // key after span-key extractor manipulation
    Map<String, List<String>> producerTopicsMappings; // producer with list of topics after span-key extractor manipulation

    public Record(String message, String key, Map<String, List<String>> producerTopicsMappings) {
        this.message = message;
        this.key = key;
        this.producerTopicsMappings = producerTopicsMappings;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, List<String>> getProducerTopicsMappings() {
        return producerTopicsMappings;
    }

    public String getKey() {
        return key;
    }
}
