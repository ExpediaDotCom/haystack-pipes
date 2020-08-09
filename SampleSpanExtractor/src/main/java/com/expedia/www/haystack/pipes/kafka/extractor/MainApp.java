package com.expedia.www.haystack.pipes.kafka.extractor;

import com.expedia.www.haystack.pipes.commons.key.extractor.loader.SpanKeyExtractorLoader;

public class MainApp {

    public static void main(String[] args) {
        SpanKeyExtractorLoader spanKeyExtractorLoader = SpanKeyExtractorLoader.getInstance();
        spanKeyExtractorLoader.getSpanKeyExtractor().forEach(spanKeyExtractor -> {
            System.out.println(spanKeyExtractor.getKey());
        });
    }
}
