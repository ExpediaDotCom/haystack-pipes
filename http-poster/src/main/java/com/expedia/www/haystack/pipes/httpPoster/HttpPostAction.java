package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.springframework.stereotype.Component;

@Component
public class HttpPostAction implements ForeachAction<String, Span> {
    private final ContentCollector contentCollector;
    private final MetricObjects metricObjects;
    private final Printer printer;
    private final Counter requestCounter;

    HttpPostAction(ContentCollector contentCollector,
                   MetricObjects metricObjects,
                   Printer printer,
                   Counter requestCounter) {
        this.contentCollector = contentCollector;
        this.metricObjects = metricObjects;
        this.printer = printer;
        this.requestCounter = requestCounter;
    }

    @Override
    public void apply(String key, Span value) {
        requestCounter.increment();
        // TODO Post to HTTP endpoint
    }
}
