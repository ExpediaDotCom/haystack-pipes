package com.expedia.www.haystack.pipes.secretDetector;

import com.amazonaws.services.s3.AmazonS3;
import com.expedia.www.haystack.commons.secretDetector.WhiteListConfig;
import com.expedia.www.haystack.commons.secretDetector.span.SpanS3ConfigFetcher;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SpringWiredS3ConfigFetcher extends SpanS3ConfigFetcher {
    @Autowired
    SpringWiredS3ConfigFetcher(Logger spanS3ConfigFetcherLogger,
                               WhiteListConfig whiteListConfig,
                               AmazonS3 amazonS3,
                               SpanS3ConfigFetcher.SpanFactory s3ConfigFetcherFactory) {
        super(spanS3ConfigFetcherLogger, whiteListConfig, amazonS3, s3ConfigFetcherFactory);
    }
}
