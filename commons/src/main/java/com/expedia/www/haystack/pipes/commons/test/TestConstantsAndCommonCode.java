package com.expedia.www.haystack.pipes.commons.test;

import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import java.util.Random;

/**
 * Constants used by tests in subpackages, included in functional code to avoid having to publish a jar file from the
 * test directory.
 */
public interface TestConstantsAndCommonCode {
    Random RANDOM = new Random();
    String EXCEPTION_MESSAGE = RANDOM.nextLong() + "EXCEPTION_MESSAGE";

    String LOGS = "[{\"timestamp\":\"234567890\",\"fields\":" +
            "[{\"key\":\"strField\",\"vStr\":\"logFieldValue\"},{\"key\":\"longField\",\"vLong\":\"4567890\"}]},"
            + "{\"timestamp\":\"234567891\",\"fields\":" +
            "[{\"key\":\"doubleField\",\"vDouble\":6.54321},{\"key\":\"boolField\",\"vBool\":false}]}],";
    String STRING_TAG_VALUE = "tagValue";
    String TAGS = "[" +
            "{\"key\":\"strKey\",\"vStr\":\"" + STRING_TAG_VALUE +"\"}," +
            "{\"key\":\"longKey\",\"vLong\":\"987654321\"}," +
            "{\"key\":\"doubleKey\",\"vDouble\":9876.54321}," +
            "{\"key\":\"boolKey\",\"vBool\":true}," +
            "{\"key\":\"bytesKey\",\"vBytes\":\"AAEC/f7/\"}]}";
    String BOGUS_TAGS = "[{\"key\":\"bogusKey\",\"vBogus\":\"bogusValue\"}]}";
    String TAGS_WITHOUT_TAG_KEY = "[{\"vBogus\":\"bogusValue\"}]}";
    String FLATTENED_TAGS = "{"
            + "\"strKey\":\"tagValue\","
            + "\"longKey\":987654321,"
            + "\"doubleKey\":9876.54321,"
            + "\"boolKey\":true,"
            + "\"bytesKey\":\"AAEC/f7/\"}}\n";
    String SPAN_ID = "unique-span-id";
    String TRACE_ID = "unique-trace-id";
    String SERVICE_NAME = "unique-service-name";
    String OPERATION_NAME = "operation-name";
    String JSON_SPAN_STRING = "{\"traceId\":\"" + TRACE_ID + "\"," +
            "\"spanId\":\"" + SPAN_ID + "\"," +
            "\"parentSpanId\":\"unique-parent-span-id\"," +
            "\"serviceName\":\"" + SERVICE_NAME + "\"," +
            "\"operationName\":\"" + OPERATION_NAME + "\"," +
            "\"startTime\":\"123456789\"," +
            "\"duration\":\"234\"," +
            "\"logs\":" + LOGS +
            "\"tags\":" + TAGS;
    String JSON_SPAN_STRING_WITH_FLATTENED_TAGS = JSON_SPAN_STRING.replace(TAGS, FLATTENED_TAGS);
    String JSON_SPAN_STRING_WITH_NO_TAGS = JSON_SPAN_STRING.replace(",\"tags\":" + TAGS, "}\n");
    String JSON_SPAN_STRING_WITH_EMPTY_TAGS = JSON_SPAN_STRING.replace(TAGS, "{}}\n");
    Span FULLY_POPULATED_SPAN = buildSpan(JSON_SPAN_STRING);
    Span NO_TAGS_SPAN = buildSpan(JSON_SPAN_STRING_WITH_NO_TAGS);
    String JSON_SPAN_STRING_WITH_BOGUS_TAGS = JSON_SPAN_STRING.replace(TAGS, BOGUS_TAGS);
    String JSON_SPAN_STRING_WITHOUT_TAG_KEY = JSON_SPAN_STRING.replace(TAGS, TAGS_WITHOUT_TAG_KEY);
    String EMAIL_ADDRESS = "haystack@expedia.com";
    String JSON_SPAN_STRING_WITH_EMAIL_ADDRESS_IN_TAG = JSON_SPAN_STRING.replace(STRING_TAG_VALUE, EMAIL_ADDRESS);
    Span EMAIL_ADDRESS_SPAN = buildSpan(JSON_SPAN_STRING_WITH_EMAIL_ADDRESS_IN_TAG);
    String IP_ADDRESS = String.format("%d.%d.%d.%d", RANDOM.nextInt(Byte.MAX_VALUE), RANDOM.nextInt(Byte.MAX_VALUE),
            RANDOM.nextInt(Byte.MAX_VALUE), RANDOM.nextInt(Byte.MAX_VALUE));
    String JSON_SPAN_STRING_WITH_IP_ADDRESS_IN_TAG = JSON_SPAN_STRING.replace(STRING_TAG_VALUE, IP_ADDRESS);
    Span IP_ADDRESS_SPAN = buildSpan(JSON_SPAN_STRING_WITH_IP_ADDRESS_IN_TAG);

    static Span buildSpan(String jsonSpanString) {
        final Span.Builder builder = Span.newBuilder();
        try {
            JsonFormat.parser().merge(jsonSpanString, builder);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse JSON", e);
        }
        return builder.build();
    }

    @SuppressWarnings("ConstantConditions")
    byte[] PROTOBUF_SPAN_BYTES = FULLY_POPULATED_SPAN.toByteArray();}
