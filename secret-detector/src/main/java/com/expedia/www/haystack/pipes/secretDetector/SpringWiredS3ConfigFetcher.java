package com.expedia.www.haystack.pipes.secretDetector;

import com.amazonaws.services.s3.AmazonS3;
import com.expedia.www.haystack.pipes.secretDetector.config.WhiteListConfig;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SpringWiredS3ConfigFetcher extends S3ConfigFetcher {
    @Autowired
    SpringWiredS3ConfigFetcher(Logger s3ConfigFetcherLogger,
                               WhiteListConfig whiteListConfig,
                               AmazonS3 amazonS3,
                               Factory s3ConfigFetcherFactory) {
        super(s3ConfigFetcherLogger, whiteListConfig, amazonS3, s3ConfigFetcherFactory);
    }
}
