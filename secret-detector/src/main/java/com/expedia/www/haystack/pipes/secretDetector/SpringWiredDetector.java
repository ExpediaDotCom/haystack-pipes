package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.www.haystack.commons.secretDetector.Detector;
import com.expedia.www.haystack.commons.secretDetector.S3ConfigFetcher;
import io.dataapps.chlorine.finder.FinderEngine;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;

@Component
public class SpringWiredDetector extends Detector {
    @Autowired
    SpringWiredDetector(Logger detectorLogger,
                        FinderEngine finderEngine,
                        Factory detectorFactory,
                        S3ConfigFetcher s3ConfigFetcher) {
        super(detectorLogger, finderEngine, detectorFactory, s3ConfigFetcher, APPLICATION);
    }
}
