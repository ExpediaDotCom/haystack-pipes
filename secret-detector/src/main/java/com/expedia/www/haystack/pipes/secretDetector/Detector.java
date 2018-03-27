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
import io.dataapps.chlorine.finder.FinderEngine;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Finds that tag keys and field keys in a Span that contain secrets.
 */
public class Detector {
    private final FinderEngine finderEngine;

    Detector(FinderEngine finderEngine) {
        this.finderEngine = finderEngine;
    }

    List<String> findSecrets(Span span) {
        final List<String> listOfKeysOfSecrets = new ArrayList<>();
        findSecretsInTags(listOfKeysOfSecrets, span);
        findSecretsInLogFields(listOfKeysOfSecrets, span);
        return listOfKeysOfSecrets;
    }

    private void findSecretsInTags(List<String> listOfKeysOfSecrets, Span span) {
        findSecrets(listOfKeysOfSecrets, span.getTagsList());
    }

    private void findSecretsInLogFields(List<String> listOfKeysOfSecrets, Span span) {
        for(final Log log : span.getLogsList()) {
            findSecrets(listOfKeysOfSecrets, log.getFieldsList());
        }
    }

    private void findSecrets(List<String> listOfKeysOfSecrets, List<Tag> tags) {
        for(final Tag tag : tags) {
            if(StringUtils.isNotEmpty(tag.getVStr())) {
                putKeysOfSecretsIntoList(listOfKeysOfSecrets, tag, finderEngine.find(tag.getVStr()));
            } else if(tag.getVBytes().size() > 0) {
                final String input = new String(tag.getVBytes().toByteArray());
                putKeysOfSecretsIntoList(listOfKeysOfSecrets, tag, finderEngine.find(input));
            }
        }
    }

    private void putKeysOfSecretsIntoList(List<String> listOfKeysOfSecrets, Tag tag, List<String> secretsList) {
        if(!secretsList.isEmpty()) {
            listOfKeysOfSecrets.add(tag.getKey());
        }
    }

}