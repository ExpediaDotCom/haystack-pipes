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

    private boolean shouldCreateNewBatchDueToDataSize(String jsonToAdd) {
        int newPayloadLength = postPayload.length();
        if(postPayloadContainsAtLeastOneRecord()) {
            newPayloadLength += 1; // comma
        }
        newPayloadLength += jsonToAdd.length();
        newPayloadLength += 1; // closing ]
        return newPayloadLength > maxBytesInPost;
    }

    String addAndReturnBatch(String jsonToAdd) {
        final String jsonToPost;
        if (shouldCreateNewBatchDueToDataSize(jsonToAdd)) {
            postPayload.append(']');
            jsonToPost = postPayload.toString();
            initialize();
            postPayload.append(jsonToAdd);
        } else {
            jsonToPost = "";
            if(postPayloadContainsAtLeastOneRecord()) {
                postPayload.append(',');
            }
            postPayload.append(jsonToAdd);
        }
        return jsonToPost;
    }

    private boolean postPayloadContainsAtLeastOneRecord() { // i.e. not just "["
        return postPayload.length() > 1;
    }
}
