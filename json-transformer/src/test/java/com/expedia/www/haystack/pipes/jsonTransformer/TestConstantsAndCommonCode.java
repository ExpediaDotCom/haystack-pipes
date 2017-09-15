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

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

class TestConstantsAndCommonCode {
    private final static String PROTOBUF_SPAN_STRING =
            "0a0f756e697175652d74726163652d6964120e756e697175652d7370616e2d69" +
            "641a15756e697175652d706172656e742d7370616e2d6964220e6f7065726174" +
            "696f6e2d6e616d6528959aef3a30ea013a3208d2f1ec6f12190a087374724669" +
            "656c641a0d6c6f674669656c6456616c756512100a096c6f6e674669656c6420" +
            "d2e696023a2c08d3f1ec6f12160a0b646f75626c654669656c6429ce70033e3f" +
            "2c1a40120d0a09626f6f6c4669656c64300042120a067374724b65791a087461" +
            "6756616c7565420f0a076c6f6e674b657920b1d1f9d60342140a09646f75626c" +
            "654b6579296ec0e787454ac340420b0a07626f6f6c4b6579300142120a086279" +
            "7465734b65793a06000102fdfeff";
    final static byte[] PROTOBUF_SPAN_BYTES = (new HexBinaryAdapter()).unmarshal(PROTOBUF_SPAN_STRING);
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
            "\"operationName\":\"operation-name\"," +
            "\"startTime\":\"123456789\"," +
            "\"duration\":\"234\"," +
            "\"logs\":" + LOGS +
            "\"tags\":" + TAGS;

    static Span createFullyPopulatedSpan() throws InvalidProtocolBufferException {
        final Span.Builder builder = Span.newBuilder();
        JsonFormat.parser().merge(JSON_SPAN_STRING, builder);
        return builder.build();
    }
}
