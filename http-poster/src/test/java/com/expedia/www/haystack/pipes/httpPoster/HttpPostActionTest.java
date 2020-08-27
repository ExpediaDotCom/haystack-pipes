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

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.TimersAndCounters;
import com.expedia.www.haystack.pipes.commons.kafka.config.HttpPostConfig;
import com.expedia.www.haystack.pipes.httpPoster.HttpPostAction.Factory;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockserver.integration.ClientAndServer;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EXCEPTION_MESSAGE;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_FLATTENED_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.httpPoster.HttpPostAction.FILTERED_IN_COUNTER_INDEX;
import static com.expedia.www.haystack.pipes.httpPoster.HttpPostAction.FILTERED_OUT_COUNTER_INDEX;
import static com.expedia.www.haystack.pipes.httpPoster.HttpPostAction.ONE_HUNDRED_PERCENT;
import static com.expedia.www.haystack.pipes.httpPoster.HttpPostAction.POSTING_ERROR_MSG;
import static com.expedia.www.haystack.pipes.httpPoster.HttpPostAction.STARTUP_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpPostActionTest {
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final String HTTP_LOCALHOST = "http://localhost:1080";
    private static final IOException IO_EXCEPTION = new IOException(EXCEPTION_MESSAGE);
    private static final String IO_EXCEPTION_MESSAGE = String.format(POSTING_ERROR_MSG, EXCEPTION_MESSAGE);
    static final Map<String, String> HEADERS = new HashMap<>(2);
    private static URL URL_;

    static {
        try {
            URL_ = new URL(HTTP_LOCALHOST);
            HEADERS.put("Content-Type", "raw");
            HEADERS.put("Content-Encoding", "gzip");
        } catch (MalformedURLException e) {
            URL_ = null;
        }
    }

    @Mock
    private Printer mockPrinter;
    @Mock
    private ContentCollector mockContentCollector;
    @Mock
    private TimersAndCounters mockTimersAndCounters;
    @Mock
    private Logger mockLogger;
    @Mock
    private HttpPostConfig mockHttpPostConfig;
    @Mock
    private Factory mockFactory;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private HttpURLConnection mockHttpURLConnection;
    @Mock
    private OutputStream mockOutputStream;
    @Mock
    private Random mockRandom;
    @Mock
    private InvalidProtocolBufferExceptionLogger mockInvalidProtocolBufferExceptionLogger;

    private HttpPostAction httpPostExternalAction;
    private Factory factory;
    private ClientAndServer mockServer;
    private int wantedNumberOfInvocationsUrl = 2;
    private int wantedNumberOfInvocationsPollPercent = 3;
    private int wantedNumberOfInvocationsInfo = 1;

    @Before
    public void setUp() {
        when(mockHttpPostConfig.getUrl()).thenReturn(HTTP_LOCALHOST);
        when(mockHttpPostConfig.getPollPercent()).thenReturn(Integer.toString(ONE_HUNDRED_PERCENT));
        final Printer realPrinter = JsonFormat.printer().omittingInsignificantWhitespace();
        httpPostExternalAction = new HttpPostAction(realPrinter, mockContentCollector, mockTimersAndCounters,
                mockLogger, mockHttpPostConfig, mockFactory, mockRandom, mockInvalidProtocolBufferExceptionLogger);
        factory = new Factory();
        mockServer = ClientAndServer.startClientAndServer(1080);
    }

    @After
    public void tearDown() {
        mockServer.stop();
        verify(mockHttpPostConfig, times(wantedNumberOfInvocationsUrl)).getUrl();
        verify(mockHttpPostConfig, times(wantedNumberOfInvocationsPollPercent)).getPollPercent();
        String msg = String.format(STARTUP_MESSAGE, HTTP_LOCALHOST, ONE_HUNDRED_PERCENT);
        verify(mockLogger, times(wantedNumberOfInvocationsInfo)).info(msg);
        verifyNoMoreInteractions(mockPrinter, mockContentCollector, mockTimersAndCounters,
                mockLogger, mockHttpPostConfig, mockFactory, mockRandom,
                mockInvalidProtocolBufferExceptionLogger);
        verifyNoMoreInteractions(mockStopwatch, mockHttpURLConnection, mockOutputStream);
    }

    @Test
    public void testApplyEmptyBatch() {
        wantedNumberOfInvocationsUrl = 1;
        wantedNumberOfInvocationsPollPercent = 2;
        when(mockContentCollector.addAndReturnBatch(anyString())).thenReturn("");

        httpPostExternalAction.apply(KEY, FULLY_POPULATED_SPAN);

        verify(mockRandom).nextInt(ONE_HUNDRED_PERCENT);
        verify(mockTimersAndCounters).incrementRequestCounter();
        verify(mockTimersAndCounters).incrementCounter(FILTERED_IN_COUNTER_INDEX);
        verify(mockTimersAndCounters).recordSpanArrivalDelta(FULLY_POPULATED_SPAN);
        verify(mockContentCollector).addAndReturnBatch(JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
    }

    @Test
    public void testApplyFilteredOut() {
        wantedNumberOfInvocationsUrl = 1;
        wantedNumberOfInvocationsPollPercent = 2;
        when(mockHttpPostConfig.getPollPercent()).thenReturn("0");

        httpPostExternalAction.apply(KEY, FULLY_POPULATED_SPAN);

        verify(mockRandom).nextInt(ONE_HUNDRED_PERCENT);
        verify(mockTimersAndCounters).incrementRequestCounter();
        verify(mockTimersAndCounters).incrementCounter(FILTERED_OUT_COUNTER_INDEX);
        verify(mockTimersAndCounters).recordSpanArrivalDelta(FULLY_POPULATED_SPAN);
    }

    @Test
    public void testApplyFullBatchHappyCase() throws IOException {
        when(mockHttpURLConnection.getOutputStream()).thenReturn(mockOutputStream);

        testApply();

        verify(mockOutputStream).write(JSON_SPAN_STRING_WITH_FLATTENED_TAGS.getBytes());
        verify(mockOutputStream).close();
        verify(mockTimersAndCounters, times(2)).incrementCounter(FILTERED_IN_COUNTER_INDEX);
    }

    @Test
    public void testApplyIOExceptionFromGetOutputStream() throws IOException {
        when(mockHttpURLConnection.getOutputStream()).thenThrow(IO_EXCEPTION);

        testApply();

        verify(mockTimersAndCounters, times(2)).incrementCounter(FILTERED_IN_COUNTER_INDEX);
        verify(mockLogger).error(IO_EXCEPTION_MESSAGE, IO_EXCEPTION);
    }

    @Test
    public void testApplyIOExceptionFromWrite() throws IOException {
        when(mockHttpURLConnection.getOutputStream()).thenReturn(mockOutputStream);
        doThrow(IO_EXCEPTION).when(mockOutputStream).write(any(byte[].class));

        testApply();

        verify(mockOutputStream).write(JSON_SPAN_STRING_WITH_FLATTENED_TAGS.getBytes());
        verify(mockOutputStream).close();
        verify(mockLogger).error(IO_EXCEPTION_MESSAGE, IO_EXCEPTION);
        verify(mockTimersAndCounters, times(2)).incrementCounter(FILTERED_IN_COUNTER_INDEX);
    }

    @Test
    public void testApplyIOExceptionFromClose() throws IOException {
        doThrow(IO_EXCEPTION).when(mockOutputStream).close();

        when(mockHttpURLConnection.getOutputStream()).thenReturn(mockOutputStream);

        testApply();

        verify(mockOutputStream).write(JSON_SPAN_STRING_WITH_FLATTENED_TAGS.getBytes());
        verify(mockOutputStream).close();
        verify(mockLogger).error(IO_EXCEPTION_MESSAGE, IO_EXCEPTION);
        verify(mockTimersAndCounters, times(2)).incrementCounter(FILTERED_IN_COUNTER_INDEX);
    }

    private void testApply() throws IOException {
        when(mockContentCollector.addAndReturnBatch(anyString()))
                .thenReturn("").thenReturn(JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
        when(mockTimersAndCounters.startTimer()).thenReturn(mockStopwatch);
        when(mockFactory.createURL(anyString())).thenReturn(URL_);
        when(mockFactory.createConnection(any(URL.class))).thenReturn(mockHttpURLConnection);
        when(mockHttpPostConfig.getHeaders()).thenReturn(HEADERS);

        httpPostExternalAction.apply(KEY, FULLY_POPULATED_SPAN);
        httpPostExternalAction.apply(KEY, FULLY_POPULATED_SPAN);

        verify(mockTimersAndCounters, times(2)).incrementRequestCounter();
        verify(mockTimersAndCounters, times(2)).recordSpanArrivalDelta(
                FULLY_POPULATED_SPAN);
        verify(mockContentCollector, times(2)).addAndReturnBatch(
                JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
        verify(mockRandom, times(2)).nextInt(ONE_HUNDRED_PERCENT);
        verify(mockTimersAndCounters).startTimer();
        verify(mockFactory).createURL(HTTP_LOCALHOST);
        verify(mockFactory).createConnection(URL_);
        verify(mockHttpURLConnection).setRequestMethod("POST");
        verify(mockHttpURLConnection).setRequestProperty(
                "Content-Length", Integer.toString(JSON_SPAN_STRING_WITH_FLATTENED_TAGS.length()));
        verify(mockHttpURLConnection).setDoOutput(true);
        verify(mockHttpPostConfig).getHeaders();
        for (Map.Entry<String, String> header : HEADERS.entrySet()) {
            verify(mockHttpURLConnection).setRequestProperty(header.getKey(), header.getValue());
        }
        verify(mockHttpURLConnection).getOutputStream();
        verify(mockStopwatch).stop();
    }

    @Test
    public void testGetBatchInvalidProtocolBufferException() throws InvalidProtocolBufferException {
        wantedNumberOfInvocationsPollPercent = 2;
        wantedNumberOfInvocationsInfo = 2;
        httpPostExternalAction = new HttpPostAction(mockPrinter, mockContentCollector, mockTimersAndCounters,
                mockLogger, mockHttpPostConfig, mockFactory, mockRandom,
                mockInvalidProtocolBufferExceptionLogger);
        final InvalidProtocolBufferException exception = new InvalidProtocolBufferException(EXCEPTION_MESSAGE);
        when(mockPrinter.print(any(Span.class))).thenThrow(exception);

        final String batch = httpPostExternalAction.getBatch(NO_TAGS_SPAN);

        assertEquals("", batch);
        verify(mockPrinter).print(NO_TAGS_SPAN);
        verify(mockInvalidProtocolBufferExceptionLogger).logError(NO_TAGS_SPAN, exception);
    }

    @Test
    public void testFactoryMethods() throws IOException {
        wantedNumberOfInvocationsUrl = 1;
        wantedNumberOfInvocationsPollPercent = 1;
        final URL url = factory.createURL(HTTP_LOCALHOST);
        assertNotNull(url);
        final HttpURLConnection httpURLConnection = factory.createConnection(url);
        assertNotNull(httpURLConnection);
    }
}
