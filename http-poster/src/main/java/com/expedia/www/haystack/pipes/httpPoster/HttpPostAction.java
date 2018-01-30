package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
class HttpPostAction implements ForeachAction<String, Span> {
    private final ContentCollector contentCollector;
    private final MetricObjects metricObjects;
    private final Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    private final Counter requestCounter;

    @Autowired
    HttpPostAction(ContentCollector contentCollector,
                   MetricObjects metricObjects,
                   Counter requestCounter) {
        this.contentCollector = contentCollector;
        this.metricObjects = metricObjects;
        this.requestCounter = requestCounter;
    }

    @Override
    public void apply(String key, Span value) {
        requestCounter.increment();
        // TODO Post to HTTP endpoint
    }
}
