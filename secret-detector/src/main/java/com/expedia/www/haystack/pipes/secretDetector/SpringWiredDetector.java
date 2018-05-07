package com.expedia.www.haystack.pipes.secretDetector;

import io.dataapps.chlorine.finder.FinderEngine;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SpringWiredDetector extends Detector {
    @Autowired
    SpringWiredDetector(Logger detectorLogger,
                        FinderEngine finderEngine,
                        Factory detectorFactory,
                        S3ConfigFetcher s3ConfigFetcher) {
        super(detectorLogger, finderEngine, detectorFactory, s3ConfigFetcher);
    }
}
