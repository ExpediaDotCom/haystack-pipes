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

import com.expedia.open.tracing.Span;
import com.netflix.servo.monitor.Counter;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FirehoseAction implements ForeachAction<String, Span> {
    private final Counter requestCounter;

    @Autowired
    FirehoseAction(Counter requestCounter) {
        this.requestCounter = requestCounter;
    }

    @Override
    public void apply(String key, Span span) {
        requestCounter.increment();
        // TODO Send data to Firehose
    }
}
