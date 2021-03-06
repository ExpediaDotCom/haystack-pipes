/*
 * Copyright 2020 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.key.extractor.loader;

import com.expedia.www.haystack.pipes.key.extractor.SpanKeyExtractor;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;


public class SpanKeyExtractorLoader {

    private static final Logger logger = LoggerFactory.getLogger(SpanKeyExtractorLoader.class);
    private static SpanKeyExtractorLoader spanKeyExtractorLoader = null;
    private List<SpanKeyExtractor> spanKeyExtractorList;
    private ServiceLoader<SpanKeyExtractor> serviceLoader;

    private SpanKeyExtractorLoader() {
        spanKeyExtractorList = new ArrayList<>();
    }

    public static synchronized SpanKeyExtractorLoader getInstance() {
        if (spanKeyExtractorLoader == null) {
            spanKeyExtractorLoader = new SpanKeyExtractorLoader();
            spanKeyExtractorLoader.loadFiles();
        }
        return spanKeyExtractorLoader;
    }

    private void loadFiles() {
        try {
            final File[] extractorFile = new File("extractors/").listFiles();
            if (extractorFile != null) {
                final List<URL> urls = new ArrayList<>();
                for (final File file : extractorFile) {
                    urls.add(file.toURI().toURL());
                }
                URLClassLoader urlClassLoader = new URLClassLoader(urls.toArray(new URL[0]), SpanKeyExtractor.class.getClassLoader());
                serviceLoader = ServiceLoader.load(SpanKeyExtractor.class, urlClassLoader);
            }
        } catch (Exception ex) {
            logger.error("Could not create the class loader for finding jar ", ex);
        } catch (NoClassDefFoundError ex) {
            logger.error("Could not find the class ", ex);
        }
    }

    public List<SpanKeyExtractor> getSpanKeyExtractor(Map<String, Config> spanKeyExtractorConfigs) {
        if (spanKeyExtractorList.isEmpty() && this.serviceLoader != null) {
            serviceLoader.forEach(spanKeyExtractor -> {
                try {
                    spanKeyExtractor.configure(spanKeyExtractorConfigs.getOrDefault(spanKeyExtractor.name(), null));
                    spanKeyExtractorList.add(spanKeyExtractor);
                    logger.debug("Extractor class is loaded: {}, at path: {}", spanKeyExtractor.name(), "extractors/");
                } catch (Exception e) {
                    logger.error("Failed to load Span Extractor, Exception: {}", e.getMessage());
                }
            });
        }
        return spanKeyExtractorList;
    }


}
