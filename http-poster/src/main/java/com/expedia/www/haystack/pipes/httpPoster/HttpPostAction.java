package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.CommonConstants;
import com.expedia.www.haystack.pipes.commons.CountersAndTimer;
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat.Printer;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Random;

@Component
class HttpPostAction implements ForeachAction<String, Span> {
    @VisibleForTesting
    static final String STARTUP_MESSAGE = "Instantiating HttpPostAction into URL [%s] with filtering of [%s] percent";
    @VisibleForTesting
    static final String POSTING_ERROR_MSG = "Exception posting to HTTP; received message [%s]";
    @VisibleForTesting
    static final int ONE_HUNDRED_PERCENT = 100;

    @VisibleForTesting
    final static int FILTERED_OUT_COUNTER_INDEX = 0;
    @VisibleForTesting
    final static int FILTERED_IN_COUNTER_INDEX = 1;

    private final TagFlattener tagFlattener = new TagFlattener();
    private final Printer printer;
    private final ContentCollector contentCollector;
    private final CountersAndTimer countersAndTimer;
    private final Logger httpPostActionLogger;
    private final HttpPostConfigurationProvider httpPostConfigurationProvider;
    private final Factory factory;
    private final Random random;

    @Autowired
    HttpPostAction(Printer printer,
                   ContentCollector contentCollector,
                   CountersAndTimer countersAndTimer,
                   Logger httpPostActionLogger,
                   HttpPostConfigurationProvider httpPostConfigurationProvider,
                   Factory httpPostActionFactory,
                   Random random) {
        this.printer = printer;
        this.contentCollector = contentCollector;
        this.countersAndTimer = countersAndTimer;
        this.httpPostActionLogger = httpPostActionLogger;
        this.httpPostConfigurationProvider = httpPostConfigurationProvider;
        this.factory = httpPostActionFactory;
        this.random = random;

        String msg = String.format(STARTUP_MESSAGE,
                this.httpPostConfigurationProvider.url(), this.httpPostConfigurationProvider.pollpercent());
        this.httpPostActionLogger.info(msg);
    }

    @Override
    public void apply(String key, Span span) {
        countersAndTimer.incrementRequestCounter();
        if(random.nextInt(ONE_HUNDRED_PERCENT) < Integer.parseInt(httpPostConfigurationProvider.pollpercent())) {
            countersAndTimer.incrementCounter(FILTERED_IN_COUNTER_INDEX);
            final String batch = getBatch(span);
            if (!StringUtils.isEmpty(batch)) {
                final Stopwatch stopwatch = countersAndTimer.startTimer();
                try (final OutputStream outputStream = getOutputStream(batch)) {
                    outputStream.write(batch.getBytes());
                } catch (Exception exception) {
                    // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
                    final String message = String.format(POSTING_ERROR_MSG, exception.getMessage());
                    httpPostActionLogger.error(message, exception);
                } finally {
                    stopwatch.stop();
                }
            }
        } else {
            countersAndTimer.incrementCounter(FILTERED_OUT_COUNTER_INDEX);
        }
    }

    @VisibleForTesting
    String getBatch(Span span) {
        String jsonWithFlattenedTags;
        final String jsonWithOpenTracingTags;
        try {
            jsonWithOpenTracingTags = printer.print(span);
            jsonWithFlattenedTags = tagFlattener.flattenTags(jsonWithOpenTracingTags);
            return contentCollector.addAndReturnBatch(jsonWithFlattenedTags);
        } catch (InvalidProtocolBufferException exception) {
            // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
            final String message = String.format(CommonConstants.PROTOBUF_ERROR_MSG, span.toString(), exception.getMessage());
            httpPostActionLogger.error(message, exception);
            return "";
        }
    }

    private HttpURLConnection getUrlConnection() throws IOException {
        final String url = httpPostConfigurationProvider.url();
        final URL factoryURL = factory.createURL(url);
        return factory.createConnection(factoryURL);
    }

    private OutputStream getOutputStream(String batch) throws IOException {
        final HttpURLConnection httpURLConnection = getUrlConnection();
        httpURLConnection.setDoOutput(true);
        httpURLConnection.setRequestMethod("POST");
        setHeaders(batch, httpURLConnection);
        return httpURLConnection.getOutputStream();
    }

    private void setHeaders(String batch, HttpURLConnection httpURLConnection) {
        httpURLConnection.setRequestProperty("Content-Length", Integer.toString(batch.length()));
        for (Map.Entry<String, String> header : httpPostConfigurationProvider.headers().entrySet()) {
            httpURLConnection.setRequestProperty(header.getKey(), header.getValue());
        }
    }

    static class Factory {
        URL createURL(String url) throws MalformedURLException {
            return new URL(url);
        }

        HttpURLConnection createConnection(URL url) throws IOException {
            return (HttpURLConnection) url.openConnection();
        }
    }
}
