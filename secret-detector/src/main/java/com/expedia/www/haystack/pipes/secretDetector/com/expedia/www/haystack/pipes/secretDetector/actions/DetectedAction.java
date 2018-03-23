package com.expedia.www.haystack.pipes.secretDetector.com.expedia.www.haystack.pipes.secretDetector.actions;

import com.expedia.open.tracing.Span;

import java.util.List;

public interface DetectedAction {
    void send(Span span, List<String> listOfKeysOfSecrets);
}
