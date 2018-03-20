package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import io.dataapps.chlorine.finder.FinderEngine;

import java.util.List;

public class Detector {
    private final FinderEngine finderEngine;

    Detector(FinderEngine finderEngine) {
        this.finderEngine = finderEngine;
    }

    List<String> findSecrets(Span span) {
        final String spanString = span.toString();
        return finderEngine.find(spanString);
    }
}
