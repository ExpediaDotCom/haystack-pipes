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
package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.commons.secretDetector.span.SpanDetector;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.expedia.www.haystack.pipes.secretDetector.actions.DetectedAction;
import com.expedia.www.haystack.pipes.secretDetector.config.ActionsConfigurationProvider;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class DetectorAction implements ForeachAction<String, Span> {
    @VisibleForTesting
    static final String CONFIDENTIAL_DATA_MSG =
            "Confidential data has been found for service [%s] operation [%s] span [%s] trace [%s] tag(s) [%s]";
    private final CountersAndTimer countersAndTimer;
    private final SpanDetector spanDetector;
    private final Logger detectorActionLogger;
    private final ActionsConfigurationProvider actionsConfigurationProvider;

    @Autowired
    public DetectorAction(CountersAndTimer countersAndTimer,
                          SpanDetector springWiredDetector,
                          Logger detectorActionLogger,
                          ActionsConfigurationProvider actionsConfigurationProvider) {
        this.countersAndTimer = countersAndTimer;
        this.spanDetector = springWiredDetector;
        this.detectorActionLogger = detectorActionLogger;
        this.actionsConfigurationProvider = actionsConfigurationProvider;
    }

    @Override
    public void apply(String key, Span span) {
        countersAndTimer.incrementRequestCounter();
        countersAndTimer.recordSpanArrivalDelta(span);
        final Stopwatch stopwatch = countersAndTimer.startTimer();
        try {
            final Map<String, List<String>> mapOfTypeToKeysOfSecrets = spanDetector.findSecrets(span);
            if (!mapOfTypeToKeysOfSecrets.isEmpty()) {
                final List<DetectedAction> detectedActions = actionsConfigurationProvider.getDetectedActions();
                detectorActionLogger.info(String.format(CONFIDENTIAL_DATA_MSG, span.getServiceName(),
                        span.getOperationName(), span.getSpanId(), span.getTraceId(), mapOfTypeToKeysOfSecrets));
                for (DetectedAction detectedAction : detectedActions) {
                    detectedAction.send(span, mapOfTypeToKeysOfSecrets);
                }

            }
        } finally {
            stopwatch.stop();
        }
    }
}
