package com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.loader;

import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.SpanKeyExtractor;
import com.expedia.www.haystack.pipes.commons.decorators.keyExtractor.config.SpanKeyExtractorConfig;
import org.slf4j.Logger;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ServiceLoader;


public class SpanKeyExtractorLoader{

    private Logger logger;
    private SpanKeyExtractorConfig keyExtractorConfig;
    private SpanKeyExtractor spanKeyExtractor;
    private ServiceLoader<SpanKeyExtractor> serviceLoader;
    private String producer;
    private static SpanKeyExtractorLoader spanKeyExtractorLoader = null;//why?

    private SpanKeyExtractorLoader(SpanKeyExtractorConfig keyExtractorConfig, String producer){
        //this.logger = Logger.;
        this.keyExtractorConfig = keyExtractorConfig;
        this.producer = producer;
    }

    public static synchronized SpanKeyExtractorLoader getInstance( SpanKeyExtractorConfig keyExtractorConfig, String producer) {
        if (spanKeyExtractorLoader == null) {
            spanKeyExtractorLoader = new SpanKeyExtractorLoader(keyExtractorConfig, producer);
        }

        spanKeyExtractorLoader.loadFiles();
        return spanKeyExtractorLoader;
    }

    private void loadFiles() {
        try {
            final File[] extractorFile = new File(keyExtractorConfig.directory()).listFiles();
            if (extractorFile!=null && extractorFile.length > 0) {
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
        if(spanKeyExtractor == null){
            if(keyExtractorConfig!=null && keyExtractorConfig.fileName().equals(serviceLoader.iterator().next().name())){
                spanKeyExtractor = serviceLoader.iterator().next();
            }
        }
        return spanKeyExtractor;
    }
}