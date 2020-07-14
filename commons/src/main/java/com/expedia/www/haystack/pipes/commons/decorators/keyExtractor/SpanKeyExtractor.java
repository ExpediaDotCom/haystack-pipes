package com.expedia.www.haystack.pipes.commons.decorators.keyExtractor;

import com.expedia.open.tracing.Span;

public interface SpanKeyExtractor {

    String name();

    public void configure();

    public String extract(Span span);

    public String getKey();

    public String getTopic();

}
