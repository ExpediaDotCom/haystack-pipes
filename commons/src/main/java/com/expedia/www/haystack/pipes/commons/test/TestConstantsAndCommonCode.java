package com.expedia.www.haystack.pipes.commons.test;

import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import java.util.Base64;
import java.util.Random;

/**
 * Constants used by tests in subpackages; this class is included in functional code to avoid having to publish a jar
 * file from the test directory.
 */
public interface TestConstantsAndCommonCode {
    Random RANDOM = new Random();
    String EXCEPTION_MESSAGE = RANDOM.nextLong() + "EXCEPTION_MESSAGE";

    String STRING_FIELD_KEY = "logStrField";
    String STRING_FIELD_VALUE = "logFieldValue";
    String BYTES_FIELD_KEY = "logBytesKey";
    String BASE_64_ENCODED_STRING = "AAEC/f7/";
    String LOGS = "[{\"timestamp\":\"234567890\",\"fields\":" + "[{\"key\":\"" + STRING_FIELD_KEY +
            "\",\"vStr\":\"" + STRING_FIELD_VALUE + "\"},{\"key\":\"longField\",\"vLong\":\"4567890\"}]},"
            + "{\"timestamp\":\"234567891\",\"fields\":" +
            "[{\"key\":\"doubleField\",\"vDouble\":6.54321}," +
            "{\"key\":\"" + BYTES_FIELD_KEY + "\",\"vBytes\":\"" + BASE_64_ENCODED_STRING + "\"}," +
            "{\"key\":\"boolField\",\"vBool\":false}]}],";
    String STRING_TAG_KEY = "strKey";
    String STRING_TAG_VALUE = "tagValue";
    String BYTES_TAG_KEY = "bytesKey";
    String TAGS = "[" +
            "{\"key\":\"" + STRING_TAG_KEY + "\",\"vStr\":\"" + STRING_TAG_VALUE +"\"}," +
            "{\"key\":\"longKey\",\"vLong\":\"987654321\"}," +
            "{\"key\":\"doubleKey\",\"vDouble\":9876.54321}," +
            "{\"key\":\"boolKey\",\"vBool\":true}," +
            "{\"key\":\"" + BYTES_TAG_KEY + "\",\"vBytes\":\"" + BASE_64_ENCODED_STRING + "\"}]}";
    String BOGUS_TAGS = "[{\"key\":\"bogusKey\",\"vBogus\":\"bogusValue\"}]}";
    String TAGS_WITHOUT_TAG_KEY = "[{\"vBogus\":\"bogusValue\"}]}";
    String EMAIL_ADDRESS = "haystack@expedia.com";
    String BASE_64_ENCODED_EMAIL = Base64.getEncoder().encodeToString(EMAIL_ADDRESS.getBytes());
    String FLATTENED_TAGS = "{"
            + "\"strKey\":\"tagValue\","
            + "\"longKey\":987654321,"
            + "\"doubleKey\":9876.54321,"
            + "\"boolKey\":true,"
            + "\"bytesKey\":\"" + BASE_64_ENCODED_STRING + "\"}}\n";
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
    String JSON_SPAN_STRING_WITH_EMAIL_ADDRESS_IN_TAG = JSON_SPAN_STRING.replace(STRING_TAG_VALUE, EMAIL_ADDRESS);
    Span EMAIL_ADDRESS_SPAN = buildSpan(JSON_SPAN_STRING_WITH_EMAIL_ADDRESS_IN_TAG);
    String JSON_SPAN_STRING_WITH_EMAIL_ADDRESS_IN_TAG_BYTES_AND_LOG_BYTES =
            JSON_SPAN_STRING.replace(BASE_64_ENCODED_STRING, BASE_64_ENCODED_EMAIL);
    Span EMAIL_ADDRESS_IN_TAG_BYTES_SPAN = buildSpan(JSON_SPAN_STRING_WITH_EMAIL_ADDRESS_IN_TAG_BYTES_AND_LOG_BYTES);
    String JSON_SPAN_STRING_WITH_EMAIL_ADDRESS_IN_LOG_TAG = JSON_SPAN_STRING.replace(STRING_FIELD_VALUE, EMAIL_ADDRESS);
    Span EMAIL_ADDRESS_LOG_SPAN = buildSpan(JSON_SPAN_STRING_WITH_EMAIL_ADDRESS_IN_LOG_TAG);

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
