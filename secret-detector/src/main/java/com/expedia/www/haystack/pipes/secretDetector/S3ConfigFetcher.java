package com.expedia.www.haystack.pipes.secretDetector;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.expedia.www.haystack.pipes.secretDetector.config.WhiteListConfig;
import com.expedia.www.haystack.pipes.secretDetector.config.WhiteListItem;
import com.netflix.servo.util.VisibleForTesting;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class S3ConfigFetcher {
    @VisibleForTesting
    static final String ERROR_MESSAGE = "Exception getting white list items" +
            "; whitelisted finder/service/operation/tag combinations may not be correct";
    @VisibleForTesting
    static final AtomicReference<List<WhiteListItem>> WHITE_LIST_ITEMS =
            new AtomicReference<>(new ArrayList<>());
    @VisibleForTesting
    static final String INVALID_DATA_MSG = "The line [%s] does not contain at least three semicolons to separate "
            + "finderName, String serviceName, String operationName, String tagName";

    private static final long ONE_HOUR = TimeUnit.HOURS.toMillis(1);
    private final Logger logger;
    private final String bucket;
    private final String key;
    private final AmazonS3 amazonS3;
    private final Factory factory;
    @VisibleForTesting
    AtomicLong lastUpdateTime = new AtomicLong(0L);
    @VisibleForTesting
    AtomicBoolean isUpdateInProgress = new AtomicBoolean(false);

    @Autowired
    S3ConfigFetcher(Logger s3ConfigFetcherLogger,
                    WhiteListConfig whiteListConfig,
                    AmazonS3 amazonS3,
                    Factory s3ConfigFetcherFactory) {
        this.logger = s3ConfigFetcherLogger;
        this.bucket = whiteListConfig.bucket();
        this.key = whiteListConfig.key();
        this.amazonS3 = amazonS3;
        this.factory = s3ConfigFetcherFactory;
    }

    List<WhiteListItem> getWhiteListItems() {
        final long now = factory.createCurrentTimeMillis();
        if(now - lastUpdateTime.get() > ONE_HOUR) {
            if (isUpdateInProgress.compareAndSet(false, true)) {
                try {
                    WHITE_LIST_ITEMS.set(readAllWhiteListItemsFromS3());
                    lastUpdateTime.set(now);
                } catch (InvalidWhitelistItemInputException e) {
                    logger.error(e.getMessage(), e);
                } catch (Exception e) {
                    logger.error(ERROR_MESSAGE, e);
                } finally {
                    isUpdateInProgress.set(false);
                }
            }
        }
        return WHITE_LIST_ITEMS.get();
    }

    private List<WhiteListItem> readAllWhiteListItemsFromS3() throws IOException, InvalidWhitelistItemInputException {
        try(final S3Object s3Object = amazonS3.getObject(bucket, key)) {
            final BufferedReader bufferedReader = getBufferedReader(s3Object);
            final List<WhiteListItem> whiteListItems = new ArrayList<>();
            WhiteListItem whiteListItem = readSingleWhiteListItemFromS3(bufferedReader);
            while(whiteListItem != null) {
                whiteListItems.add(whiteListItem);
                whiteListItem = readSingleWhiteListItemFromS3(bufferedReader);
            }
            return whiteListItems;
        }
    }

    private BufferedReader getBufferedReader(S3Object s3Object) {
        final InputStream inputStream = s3Object.getObjectContent();
        final InputStreamReader inputStreamReader = factory.createInputStreamReader(inputStream);
        return factory.createBufferedReader(inputStreamReader);
    }

    /**
     * Reads a line from S3 and transforms it to a WhiteListItem
     * @param reader the reader
     * @return a non-null WhiteListItem if the read was successful, else null (which indicates all lines have been read)
     * @throws IOException if a problem occurs reading from S3
     * @throws InvalidWhitelistItemInputException if an input line in the S3 file is not formatted properly
     */
    private WhiteListItem readSingleWhiteListItemFromS3(BufferedReader reader)
            throws IOException, InvalidWhitelistItemInputException {
        final String line = reader.readLine();
        if (line == null) {
            return null;
        }
        final String[] strings = line.split(";");
        if(strings.length >= 4) {
            return new WhiteListItem(strings[0], strings[1], strings[2], strings[3]);
        }
        throw new InvalidWhitelistItemInputException(line);
    }

    static class InvalidWhitelistItemInputException extends Exception {
        InvalidWhitelistItemInputException(String line) {
            super(String.format(INVALID_DATA_MSG, line));
        }
    }

    static class Factory {
        long createCurrentTimeMillis() {
            return System.currentTimeMillis();
        }

        InputStreamReader createInputStreamReader(InputStream inputStream) {
            return new InputStreamReader(inputStream);
        }

        BufferedReader createBufferedReader(InputStreamReader inputStreamReader) {
            return new BufferedReader(inputStreamReader);
        }
    }
}
