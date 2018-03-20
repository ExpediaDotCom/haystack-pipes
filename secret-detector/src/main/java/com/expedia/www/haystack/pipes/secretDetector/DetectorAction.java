package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;

import java.util.List;

public class DetectorAction implements ForeachAction<String, Span> {
    @VisibleForTesting
    static final String CONFIDENTIAL_DATA_MSG =
            "Confidential data has been found for service [%s] operation [%s] span [%s] trace [%s]";
    private final CountersAndTimer countersAndTimer;
    private final Detector detector;
    private final Logger detectorLogger;

    public DetectorAction(CountersAndTimer countersAndTimer, Detector detector, Logger detectorLogger) {
        this.countersAndTimer = countersAndTimer;
        this.detector = detector;
        this.detectorLogger = detectorLogger;
    }

    @Override
    public void apply(String key, Span span) {
        countersAndTimer.incrementRequestCounter();
        final Stopwatch stopwatch = countersAndTimer.startTimer();
        try {
            final List<String> secrets = detector.findSecrets(span);
            if (!secrets.isEmpty()) {
                detectorLogger.info(String.format(CONFIDENTIAL_DATA_MSG,
                        span.getServiceName(), span.getOperationName(), span.getSpanId(), span.getTraceId()));
            }
        } finally {
            stopwatch.stop();
        }
    }
}
