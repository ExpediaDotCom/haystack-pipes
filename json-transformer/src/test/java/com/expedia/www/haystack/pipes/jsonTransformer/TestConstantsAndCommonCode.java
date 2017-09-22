/*
 * Copyright 2017 Expedia, Inc.
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
package com.expedia.www.haystack.pipes.jsonTransformer;

import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

class TestConstantsAndCommonCode {
    private static final String LOGS = "[{\"timestamp\":\"234567890\",\"fields\":" +
            "[{\"key\":\"strField\",\"vStr\":\"logFieldValue\"},{\"key\":\"longField\",\"vLong\":\"4567890\"}]},"
            + "{\"timestamp\":\"234567891\",\"fields\":" +
            "[{\"key\":\"doubleField\",\"vDouble\":6.54321},{\"key\":\"boolField\",\"vBool\":false}]}],";
    private static final String TAGS = "[" +
            "{\"key\":\"strKey\",\"vStr\":\"tagValue\"}," +
            "{\"key\":\"longKey\",\"vLong\":\"987654321\"}," +
            "{\"key\":\"doubleKey\",\"vDouble\":9876.54321}," +
            "{\"key\":\"boolKey\",\"vBool\":true}," +
            "{\"key\":\"bytesKey\",\"vBytes\":\"AAEC/f7/\"}]}";
    final static String JSON_SPAN_STRING = "{\"traceId\":\"unique-trace-id\"," +
            "\"spanId\":\"unique-span-id\"," +
            "\"parentSpanId\":\"unique-parent-span-id\"," +
            "\"serviceName\":\"unique-service-name\"," +
            "\"operationName\":\"operation-name\"," +
            "\"startTime\":\"123456789\"," +
            "\"duration\":\"234\"," +
            "\"logs\":" + LOGS +
            "\"tags\":" + TAGS;
    final static Span FULLY_POPULATED_SPAN;
    static {
        final Span.Builder builder = Span.newBuilder();
        try {
            JsonFormat.parser().merge(JSON_SPAN_STRING, builder);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse JSON", e);
        }
        FULLY_POPULATED_SPAN = builder.build();
    }
    @SuppressWarnings("ConstantConditions")
    final static byte[] PROTOBUF_SPAN_BYTES = FULLY_POPULATED_SPAN.toByteArray();
}
