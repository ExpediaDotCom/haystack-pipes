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
import com.expedia.www.haystack.pipes.commons.kafka.TagFlattener;
import com.expedia.www.haystack.pipes.commons.kafka.config.HttpPostConfig;
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
    static final int FILTERED_OUT_COUNTER_INDEX = 0;
    @VisibleForTesting
    static final int FILTERED_IN_COUNTER_INDEX = 1;

    private final TagFlattener tagFlattener = new TagFlattener();
    private final Printer printer;
    private final ContentCollector contentCollector;
    private final TimersAndCounters timersAndCounters;
    private final Logger httpPostActionLogger;
    private final HttpPostConfig httpPostConfig;
    private final Factory factory;
    private final Random random;
    private final InvalidProtocolBufferExceptionLogger invalidProtocolBufferExceptionLogger;

    @Autowired
    HttpPostAction(Printer printer,
                   ContentCollector contentCollector,
                   TimersAndCounters timersAndCounters,
                   Logger httpPostActionLogger,
                   HttpPostConfig httpPostConfig,
                   Factory httpPostActionFactory,
                   Random random,
                   InvalidProtocolBufferExceptionLogger invalidProtocolBufferExceptionLogger) {
        this.printer = printer;
        this.contentCollector = contentCollector;
        this.timersAndCounters = timersAndCounters;
        this.httpPostActionLogger = httpPostActionLogger;
        this.httpPostConfig = httpPostConfig;
        this.factory = httpPostActionFactory;
        this.random = random;
        this.invalidProtocolBufferExceptionLogger = invalidProtocolBufferExceptionLogger;

        String msg = String.format(STARTUP_MESSAGE,
                this.httpPostConfig.getUrl(), this.httpPostConfig.getPollPercent());
        this.httpPostActionLogger.info(msg);
    }

    @Override
    public void apply(String key, Span span) {
        timersAndCounters.incrementRequestCounter();
        timersAndCounters.recordSpanArrivalDelta(span);
        if(random.nextInt(ONE_HUNDRED_PERCENT) < Integer.parseInt(httpPostConfig.getPollPercent())) {
            timersAndCounters.incrementCounter(FILTERED_IN_COUNTER_INDEX);
            final String batch = getBatch(span);
            if (!StringUtils.isEmpty(batch)) {
                final Stopwatch stopwatch = timersAndCounters.startTimer();
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
            timersAndCounters.incrementCounter(FILTERED_OUT_COUNTER_INDEX);
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
            invalidProtocolBufferExceptionLogger.logError(span, exception);
            return "";
        }
    }

    private HttpURLConnection getUrlConnection() throws IOException {
        final String url = httpPostConfig.getUrl();
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
        for (Map.Entry<String, String> header : httpPostConfig.getHeaders().entrySet()) {
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
