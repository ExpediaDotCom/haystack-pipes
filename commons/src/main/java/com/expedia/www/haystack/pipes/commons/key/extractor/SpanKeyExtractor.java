package com.expedia.www.haystack.pipes.commons.key.extractor;

import com.expedia.open.tracing.Span;
import java.util.List;

public interface SpanKeyExtractor {

    String name();

    public void configure(String config);

    public String extract(Span span);

    public String getKey();

    public List<String> getTopics();

}
