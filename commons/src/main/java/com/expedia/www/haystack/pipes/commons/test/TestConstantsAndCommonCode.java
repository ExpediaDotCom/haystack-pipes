package com.expedia.www.haystack.pipes.commons.test;

import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

/**
 * Constants used by tests in subpackages, included in functional code to avoid having to publish a jar file from the
 * test directory.
 */
public class TestConstantsAndCommonCode {
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
    private static final String BOGUS_TAGS = "[{\"key\":\"bogusKey\",\"vBogus\":\"bogusValue\"}]}";
    private static final String FLATTENED_TAGS = "{"
            + "\"strKey\":\"tagValue\","
            + "\"longKey\":987654321,"
            + "\"doubleKey\":9876.54321,"
            + "\"boolKey\":true,"
            + "\"bytesKey\":\"AAEC/f7/\"}}";
    public final static String JSON_SPAN_STRING = "{\"traceId\":\"unique-trace-id\"," +
            "\"spanId\":\"unique-span-id\"," +
            "\"parentSpanId\":\"unique-parent-span-id\"," +
            "\"serviceName\":\"unique-service-name\"," +
            "\"operationName\":\"operation-name\"," +
            "\"startTime\":\"123456789\"," +
            "\"duration\":\"234\"," +
            "\"logs\":" + LOGS +
            "\"tags\":" + TAGS;
    public static final String JSON_SPAN_STRING_WITH_FLATTENED_TAGS = JSON_SPAN_STRING.replace(TAGS, FLATTENED_TAGS);
    public static final String JSON_SPAN_STRING_WITH_NO_TAGS = JSON_SPAN_STRING.replace(",\"tags\":" + TAGS, "}");
    public static final String JSON_SPAN_STRING_WITH_EMPTY_TAGS = JSON_SPAN_STRING.replace(TAGS, "{}}");
    public static final Span FULLY_POPULATED_SPAN = buildSpan(JSON_SPAN_STRING);
    public static final Span NO_TAGS_SPAN = buildSpan(JSON_SPAN_STRING_WITH_NO_TAGS);
    public static final String JSON_SPAN_STRING_WITH_BOGUS_TAGS = JSON_SPAN_STRING.replace(TAGS, BOGUS_TAGS);

    private static Span buildSpan(String jsonSpanString) {
        final Span.Builder builder = Span.newBuilder();
        try {
            JsonFormat.parser().merge(jsonSpanString, builder);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse JSON", e);
        }
        return builder.build();
    }

    @SuppressWarnings("ConstantConditions")
    public final static byte[] PROTOBUF_SPAN_BYTES = FULLY_POPULATED_SPAN.toByteArray();}
