/*
 * Copyright 2018 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Log;
import com.expedia.open.tracing.Span;
import com.expedia.open.tracing.Tag;
import com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction;
import com.google.common.annotations.VisibleForTesting;
import io.dataapps.chlorine.finder.FinderEngine;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Finds that tag keys and field keys in a Span that contain secrets.
 */
@Component
public class Detector implements ValueMapper<Span, Iterable<String>> {
    private final FinderEngine finderEngine;
    private final Logger logger;

    @Autowired
    Detector(Logger detectorLogger, FinderEngine finderEngine) {
        this.logger = detectorLogger;
        this.finderEngine = finderEngine;
    }

    @VisibleForTesting
    Map<String, List<String>> findSecrets(Span span) {
        final Map<String, List<String>> mapOfTypeToKeysOfSecrets = new HashMap<>();
        findSecretsInTags(mapOfTypeToKeysOfSecrets, span);
        findSecretsInLogFields(mapOfTypeToKeysOfSecrets, span);
        return mapOfTypeToKeysOfSecrets;
    }

    private void findSecretsInTags(Map<String, List<String>> mapOfTypeToKeysOfSecrets, Span span) {
        findSecrets(mapOfTypeToKeysOfSecrets, span.getTagsList());
    }

    private void findSecretsInLogFields(Map<String, List<String>> mapOfTypeToKeysOfSecrets, Span span) {
        for (final Log log : span.getLogsList()) {
            findSecrets(mapOfTypeToKeysOfSecrets, log.getFieldsList());
        }
    }

    private void findSecrets(Map<String, List<String>> mapOfTypeToKeysOfSecrets, List<Tag> tags) {
        for (final Tag tag : tags) {
            if (StringUtils.isNotEmpty(tag.getVStr())) {
                putKeysOfSecretsIntoMap(mapOfTypeToKeysOfSecrets, tag, finderEngine.findWithType(tag.getVStr()));
            } else if (tag.getVBytes().size() > 0) {
                final String input = new String(tag.getVBytes().toByteArray());
                putKeysOfSecretsIntoMap(mapOfTypeToKeysOfSecrets, tag, finderEngine.findWithType(input));
            }
        }
    }

    private void putKeysOfSecretsIntoMap(Map<String, List<String>> mapOfTypeToKeysOfSecrets,
                                         Tag tag,
                                         Map<String, List<String>> mapOfTypeToKeysOfSecretsJustFound) {
        for (String key : mapOfTypeToKeysOfSecretsJustFound.keySet()) {
            mapOfTypeToKeysOfSecrets.computeIfAbsent(key, (l -> new ArrayList<>())).add(tag.getKey());
        }
    }

    @Override
    public Iterable<String> apply(Span span) {
        final Map<String, List<String>> mapOfTypeToKeysOfSecrets = findSecrets(span);
        if (mapOfTypeToKeysOfSecrets.isEmpty()) {
            return Collections.emptyList();
        }
        final String emailText = EmailerDetectedAction.getEmailText(span, mapOfTypeToKeysOfSecrets);
        logger.info(emailText);
        return Collections.singleton(emailText);
    }
}