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
package com.expedia.www.haystack.pipes.httpPoster;

import org.apache.commons.lang3.StringUtils;

class ContentCollector {
    private final int maxBytesInPost;
    private final StringBuilder postPayload;

    ContentCollector(HttpPostConfigurationProvider httpPostConfigurationProvider) {
        this.maxBytesInPost = httpPostConfigurationProvider.maxbytes();
        postPayload = new StringBuilder(maxBytesInPost);
        initialize();
    }

    private void initialize() {
        postPayload.setLength(0);
        postPayload.append('[');
    }

    private boolean shouldCreateNewBatchDueToDataSize(String trimmedJsonToAdd) {
        int newPayloadLength = postPayload.length();
        if (isCommaRequired(trimmedJsonToAdd)) {
            newPayloadLength += 1; // comma
        }
        newPayloadLength += trimmedJsonToAdd.length();
        newPayloadLength += 1; // closing ]
        return newPayloadLength > maxBytesInPost;
    }

    String addAndReturnBatch(String jsonToAdd) {
        final String trimmedJsonToAdd = jsonToAdd.trim();
        final String jsonToPost;
        if (shouldCreateNewBatchDueToDataSize(trimmedJsonToAdd)) {
            postPayload.append(']');
            jsonToPost = postPayload.toString();
            initialize();
            postPayload.append(trimmedJsonToAdd);
        } else {
            jsonToPost = "";
            if (isCommaRequired(trimmedJsonToAdd)) {
                postPayload.append(',');
            }
            postPayload.append(trimmedJsonToAdd);
        }
        return jsonToPost;
    }

    /**
     * Determine if a comma is required; this method could be changed to no longer call isBlank() if the caller can be
     * trusted to never send whitespace around the JSON.
     *
     * @param jsonToAdd the JSON to add
     * @return true if the JSON is not blank
     */
    private boolean isCommaRequired(String jsonToAdd) {
        return postPayloadContainsAtLeastOneRecord() && !StringUtils.isBlank(jsonToAdd);
    }

    private boolean postPayloadContainsAtLeastOneRecord() { // i.e. not just "["
        return postPayload.length() > 1;
    }
}
