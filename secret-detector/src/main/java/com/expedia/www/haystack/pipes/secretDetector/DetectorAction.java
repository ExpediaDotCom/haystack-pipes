package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DetectorAction implements ForeachAction<String, Span> {
    @VisibleForTesting
    static final String CONFIDENTIAL_DATA_MSG =
            "Confidential data has been found for service [%s] operation [%s] span [%s] trace [%s] tag(s) [%s]";
    private final CountersAndTimer countersAndTimer;
    private final Detector detector;
    private final Logger detectorActionLogger;

    @Autowired
    public DetectorAction(CountersAndTimer countersAndTimer,
                          Detector detector,
                          Logger detectorActionLogger) {
        this.countersAndTimer = countersAndTimer;
        this.detector = detector;
        this.detectorActionLogger = detectorActionLogger;
    }

    @Override
    public void apply(String key, Span span) {
        countersAndTimer.incrementRequestCounter();
        final Stopwatch stopwatch = countersAndTimer.startTimer();
        try {
            final List<String> listOfKeysOfSecrets = detector.findSecrets(span);
            if (!listOfKeysOfSecrets.isEmpty()) {
                detectorActionLogger.info(String.format(CONFIDENTIAL_DATA_MSG, span.getServiceName(), span.getOperationName(),
                        span.getSpanId(), span.getTraceId(), listOfKeysOfSecrets));
            }
        } finally {
            stopwatch.stop();
        }
    }
}
