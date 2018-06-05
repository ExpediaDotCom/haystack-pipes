package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.www.haystack.commons.secretDetector.span.SpanNameAndCountRecorder;
import com.expedia.www.haystack.commons.secretDetector.span.SpanS3ConfigFetcher;
import com.expedia.www.haystack.commons.secretDetector.span.SpanSecretMasker;
import io.dataapps.chlorine.finder.FinderEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;

@Component
public class SpringWiredSpanSecretMasker extends SpanSecretMasker {
    @Autowired
    public SpringWiredSpanSecretMasker(FinderEngine finderEngine,
                                       SpanSecretMasker.Factory spanSecretMaskerFactory,
                                       SpanS3ConfigFetcher s3ConfigFetcher,
                                       SpanNameAndCountRecorder spanNameAndCountRecorder) {
        super(finderEngine, spanSecretMaskerFactory, s3ConfigFetcher, spanNameAndCountRecorder, APPLICATION);
    }
}
