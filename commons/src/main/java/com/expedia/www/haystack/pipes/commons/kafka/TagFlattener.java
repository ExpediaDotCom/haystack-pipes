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
package com.expedia.www.haystack.pipes.commons.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TagFlattener {
    public String flattenTags(String jsonWithOpenTracingTags) {
        final String tagsKey = "tags";
        final JsonObject jsonObject = new Gson().fromJson(jsonWithOpenTracingTags, JsonObject.class);
        final JsonElement jsonElementThatMightBeJsonArray = jsonObject.get(tagsKey);
        if (jsonElementThatMightBeJsonArray instanceof JsonArray) {
            final JsonArray jsonArray = (JsonArray) jsonObject.remove(tagsKey);
            final JsonObject flattenedTagMap = new JsonObject();
            jsonObject.add(tagsKey, flattenedTagMap);
            for (final JsonElement jsonElement : jsonArray) {
                final JsonObject tagMap = jsonElement.getAsJsonObject();
                final JsonElement keyThatMightBeNull = tagMap.get("key");
                if(keyThatMightBeNull != null) {
                    final String key = keyThatMightBeNull.getAsString();
                    final JsonElement vStr = tagMap.get("vStr");
                    if (vStr != null) {
                        flattenedTagMap.addProperty(key, vStr.getAsString());
                        continue;
                    }
                    final JsonElement vLong = tagMap.get("vLong");
                    if (vLong != null) {
                        flattenedTagMap.addProperty(key, vLong.getAsLong());
                        continue;
                    }
                    final JsonElement vDouble = tagMap.get("vDouble");
                    if (vDouble != null) {
                        flattenedTagMap.addProperty(key, vDouble.getAsDouble());
                        continue;
                    }
                    final JsonElement vBool = tagMap.get("vBool");
                    if (vBool != null) {
                        flattenedTagMap.addProperty(key, vBool.getAsBoolean());
                        continue;
                    }
                    final JsonElement vBytes = tagMap.get("vBytes");
                    if (vBytes != null) {
                        flattenedTagMap.addProperty(key, vBytes.getAsString());
                    }
                }
            }
        }
        final StringBuilder stringBuilder = new StringBuilder(jsonObject.toString());
        appendNewLineForFirehose(stringBuilder);
        return stringBuilder.toString();
    }

    /**
     * Appends a new line to the JSON, ito allow Firehose to recognize separate records. From Kapil Rastogi, who wrote a
     * Lambda on which the code in firehose-writer is based, "[The] problem WITHOUT that new line was that when firehose
     * was writing files to s3 it was not able to segregate between the records."
     *
     * @param stringBuilder the object to which a new line will be appended
     */
    private void appendNewLineForFirehose(StringBuilder stringBuilder) {
        stringBuilder.append('\n');
    }

}
