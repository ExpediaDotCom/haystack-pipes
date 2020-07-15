package com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.loader;

import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.config.SpanKeyExtractorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ServiceLoader;


public class SpanKeyExtractorLoader {

    private Logger logger;
    private SpanKeyExtractorConfig keyExtractorConfig;
    private SpanKeyExtractor spanKeyExtractor;
    private ServiceLoader<SpanKeyExtractor> serviceLoader;
    private static SpanKeyExtractorLoader spanKeyExtractorLoader = null;//why?

    private SpanKeyExtractorLoader(SpanKeyExtractorConfig keyExtractorConfig) {
        this.keyExtractorConfig = keyExtractorConfig;
        this.logger = LoggerFactory.getLogger("SpanKeyExtractorLoader");
    }

    public static synchronized SpanKeyExtractorLoader getInstance(SpanKeyExtractorConfig keyExtractorConfig) {
        if (spanKeyExtractorLoader == null) {
            spanKeyExtractorLoader = new SpanKeyExtractorLoader(keyExtractorConfig);
        }
        spanKeyExtractorLoader.loadFiles();
        return spanKeyExtractorLoader;
    }

    private void loadFiles() {
        try {
            final File[] extractorFile = new File(keyExtractorConfig.directory()).listFiles();
            if (extractorFile != null && extractorFile.length > 0) {
                URL url = extractorFile[0].toURI().toURL();
                URL[] urls = new URL[]{url};
                URLClassLoader urlClassLoader = new URLClassLoader(urls, SpanKeyExtractor.class.getClassLoader());
                serviceLoader = ServiceLoader.load(SpanKeyExtractor.class, urlClassLoader);
            }
        } catch (Exception ex) {
            logger.error("Could not create the class loader for finding jar ", ex);
        } catch (NoClassDefFoundError ex) {
            logger.error("Could not find the class ", ex);
        }
    }

    public SpanKeyExtractor getSpanKeyExtractor() {
        if (spanKeyExtractor == null) {
            if (keyExtractorConfig != null && keyExtractorConfig.fileName() != null &&
                    serviceLoader != null && serviceLoader.iterator().hasNext() &&
                    keyExtractorConfig.fileName().equals(serviceLoader.iterator().next().name())) {
                spanKeyExtractor = serviceLoader.iterator().next();
            }
        }
        return spanKeyExtractor;
    }
}
