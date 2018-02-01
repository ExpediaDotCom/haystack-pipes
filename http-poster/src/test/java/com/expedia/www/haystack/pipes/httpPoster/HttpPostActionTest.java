package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.httpPoster.HttpPostAction.Factory;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
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

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_FLATTENED_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.NO_TAGS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.httpPoster.HttpPostAction.POSTING_ERROR_MSG;
import static com.expedia.www.haystack.pipes.httpPoster.HttpPostAction.PROTOBUF_ERROR_MSG;
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
    private final static String KEY = RANDOM.nextLong() + "KEY";
    private static final String HTTP_LOCALHOST = "http://localhost:1080";
    private static final String EXCEPTION_MESSAGE = "Test Exception";
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
    private Counter mockRequestCounter;
    @Mock
    private Timer mockHttpPostTimer;
    @Mock
    private Logger mockLogger;
    @Mock
    private HttpPostConfigurationProvider mockHttpPostConfigurationProvider;
    @Mock
    private Factory mockFactory;
    @Mock
    private Stopwatch mockStopwatch;
    @Mock
    private HttpURLConnection mockHttpURLConnection;
    @Mock
    private OutputStream mockOutputStream;

    private HttpPostAction httpPostExternalAction;
    private Factory factory;
    private ClientAndServer mockServer;

    @Before
    public void setUp() {
        final Printer realPrinter = JsonFormat.printer().omittingInsignificantWhitespace();
        httpPostExternalAction = new HttpPostAction(realPrinter, mockContentCollector, mockRequestCounter,
                mockHttpPostTimer, mockLogger, mockHttpPostConfigurationProvider, mockFactory);
        factory = new Factory();
        mockServer = ClientAndServer.startClientAndServer(1080);
    }

    @After
    public void tearDown() {
        mockServer.stop();
        verifyNoMoreInteractions(mockPrinter, mockContentCollector, mockRequestCounter,
                mockHttpPostTimer, mockLogger, mockHttpPostConfigurationProvider, mockFactory,
                mockStopwatch, mockHttpURLConnection, mockOutputStream);
    }

    @Test
    public void testApplyEmptyBatch() {
        when(mockContentCollector.addAndReturnBatch(anyString())).thenReturn("");

        httpPostExternalAction.apply(KEY, FULLY_POPULATED_SPAN);

        verify(mockRequestCounter).increment();
        verify(mockContentCollector).addAndReturnBatch(JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
    }

    @Test
    public void testApplyFullBatchHappyCase() throws IOException {
        when(mockHttpURLConnection.getOutputStream()).thenReturn(mockOutputStream);

        testApply();

        verify(mockOutputStream).write(JSON_SPAN_STRING_WITH_FLATTENED_TAGS.getBytes());
        verify(mockOutputStream).close();
    }

    @Test
    public void testApplyIOExceptionFromGetOutputStream() throws IOException {
        when(mockHttpURLConnection.getOutputStream()).thenThrow(IO_EXCEPTION);

        testApply();

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
    }

    @Test
    public void testApplyIOExceptionFromClose() throws IOException {
        doThrow(IO_EXCEPTION).when(mockOutputStream).close();

        when(mockHttpURLConnection.getOutputStream()).thenReturn(mockOutputStream);

        testApply();

        verify(mockOutputStream).write(JSON_SPAN_STRING_WITH_FLATTENED_TAGS.getBytes());
        verify(mockOutputStream).close();
        verify(mockLogger).error(IO_EXCEPTION_MESSAGE, IO_EXCEPTION);
    }

    private void testApply() throws IOException {
        when(mockContentCollector.addAndReturnBatch(anyString()))
                .thenReturn("").thenReturn(JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
        when(mockHttpPostConfigurationProvider.url()).thenReturn(HTTP_LOCALHOST);
        when(mockHttpPostTimer.start()).thenReturn(mockStopwatch);
        when(mockFactory.createURL(anyString())).thenReturn(URL_);
        when(mockFactory.createConnection(any(URL.class))).thenReturn(mockHttpURLConnection);
        when(mockHttpPostConfigurationProvider.headers()).thenReturn(HEADERS);

        httpPostExternalAction.apply(KEY, FULLY_POPULATED_SPAN);
        httpPostExternalAction.apply(KEY, FULLY_POPULATED_SPAN);

        verify(mockRequestCounter, times(2)).increment();
        verify(mockContentCollector, times(2)).addAndReturnBatch(
                JSON_SPAN_STRING_WITH_FLATTENED_TAGS);
        verify(mockHttpPostTimer).start();
        verify(mockHttpPostConfigurationProvider).url();
        verify(mockFactory).createURL(HTTP_LOCALHOST);
        verify(mockFactory).createConnection(URL_);
        verify(mockHttpURLConnection).setRequestMethod("POST");
        verify(mockHttpURLConnection).setRequestProperty(
                "Content-Length", Integer.toString(JSON_SPAN_STRING_WITH_FLATTENED_TAGS.length()));
        verify(mockHttpPostConfigurationProvider).headers();
        for (Map.Entry<String, String> header : HEADERS.entrySet()) {
            verify(mockHttpURLConnection).setRequestProperty(header.getKey(), header.getValue());
        }
        verify(mockHttpURLConnection).getOutputStream();
        verify(mockStopwatch).stop();
    }

    @Test
    public void testGetBatchInvalidProtocolBufferException() throws InvalidProtocolBufferException {
        httpPostExternalAction = new HttpPostAction(mockPrinter, mockContentCollector, mockRequestCounter, mockHttpPostTimer,
                mockLogger, mockHttpPostConfigurationProvider, mockFactory);
        final InvalidProtocolBufferException exception = new InvalidProtocolBufferException(EXCEPTION_MESSAGE);
        when(mockPrinter.print(any(Span.class))).thenThrow(exception);

        final String batch = httpPostExternalAction.getBatch(NO_TAGS_SPAN);

        assertEquals("", batch);
        verify(mockPrinter).print(NO_TAGS_SPAN);
        final String message = String.format(PROTOBUF_ERROR_MSG, NO_TAGS_SPAN.toString(), EXCEPTION_MESSAGE);
        verify(mockLogger).error(message, exception);
    }

    @Test
    public void testFactoryMethods() throws IOException {
        final URL url = factory.createURL(HTTP_LOCALHOST);
        assertNotNull(url);
        final HttpURLConnection httpURLConnection = factory.createConnection(url);
        assertNotNull(httpURLConnection);
    }
}
