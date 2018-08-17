package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.commons.secretDetector.span.SpanNameAndCountRecorder;
import com.expedia.www.haystack.commons.secretDetector.span.SpanS3ConfigFetcher;
import com.expedia.www.haystack.commons.secretDetector.span.SpanSecretMasker;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import io.dataapps.chlorine.finder.FinderEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;

@Component
public class SpringWiredSpanSecretMasker extends SpanSecretMasker {
    private final CountersAndTimer countersAndTimer;

    @Autowired
    public SpringWiredSpanSecretMasker(FinderEngine finderEngine,
                                       SpanSecretMasker.Factory spanSecretMaskerFactory,
                                       SpanS3ConfigFetcher s3ConfigFetcher,
                                       CountersAndTimer countersAndTimer,
                                       SpanNameAndCountRecorder spanNameAndCountRecorder) {
        super(finderEngine, spanSecretMaskerFactory, s3ConfigFetcher, spanNameAndCountRecorder, APPLICATION);
        this.countersAndTimer = countersAndTimer;
    }

    @Override
    public Span apply(@SuppressWarnings("ParameterNameDiffersFromOverriddenParameter") Span span) {
        countersAndTimer.recordSpanArrivalDelta(span);
        return super.apply(span);
    }
}
