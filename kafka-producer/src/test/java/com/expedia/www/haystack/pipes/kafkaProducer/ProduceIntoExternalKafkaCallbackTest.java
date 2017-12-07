package com.expedia.www.haystack.pipes.kafkaProducer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Random;

import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaCallback.DEBUG_MSG;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaCallback.ERROR_MSG;
import static com.expedia.www.haystack.pipes.kafkaProducer.ProduceIntoExternalKafkaCallback.logger;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProduceIntoExternalKafkaCallbackTest {
    private final static Random RANDOM = new Random();
    private final static String TOPIC = RANDOM.nextLong() + "TOPIC";
    private final static int PARTITION = RANDOM.nextInt();
    private final static TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    private final static long BASE_OFFSET = -1;
    private final static long RELATIVE_OFFSET = RANDOM.nextLong();
    private final static long TIMESTAMP = System.currentTimeMillis();
    private final static long CHECKSUM = RANDOM.nextLong();
    private final static int SERIALIZED_KEY_SIZE = RANDOM.nextInt();
    private final static int SERIALIZED_VALUE_SIZE = RANDOM.nextInt();
    private final static String MESSAGE = RANDOM.nextLong() + "MESSAGE";

    @Mock
    private Logger mockLogger;
    private Logger realLogger;

    @Mock
    private Exception mockException;

    private RecordMetadata recordMetadata;
    private ProduceIntoExternalKafkaCallback produceIntoExternalKafkaCallback;

    @Before
    public void setUp() {
        injectMockAndSaveRealObjects();
        recordMetadata = new RecordMetadata(TOPIC_PARTITION, BASE_OFFSET, RELATIVE_OFFSET, TIMESTAMP, CHECKSUM,
                SERIALIZED_KEY_SIZE, SERIALIZED_VALUE_SIZE);
        produceIntoExternalKafkaCallback = new ProduceIntoExternalKafkaCallback();
    }

    private void injectMockAndSaveRealObjects() {
        saveRealAndInjectMockLogger();
    }

    private void saveRealAndInjectMockLogger() {
        realLogger = logger;
        logger = mockLogger;
    }

    @After
    public void tearDown() {
        restoreRealObjects();
        verifyNoMoreInteractions(mockLogger, mockException);
    }

    private void restoreRealObjects() {
        logger = realLogger;
    }

    @Test
    public void testOnCompletionBothNull() {
        produceIntoExternalKafkaCallback.onCompletion(null, null);
    }

    @Test
    public void testOnCompletionNonNullMetadataDebugDisabled() {
        when(mockLogger.isDebugEnabled()).thenReturn(false);

        produceIntoExternalKafkaCallback.onCompletion(recordMetadata, null);

        verify(mockLogger).isDebugEnabled();
    }

    @Test
    public void testOnCompletionNonNullMetadataDebugEnabled() {
        when(mockLogger.isDebugEnabled()).thenReturn(true);

        produceIntoExternalKafkaCallback.onCompletion(recordMetadata, null);

        verify(mockLogger).isDebugEnabled();
        verify(mockLogger).debug(String.format(DEBUG_MSG, TOPIC, PARTITION, BASE_OFFSET));
    }

    @Test
    public void testOneCompletionNonNullException() {
        when(mockException.getMessage()).thenReturn(MESSAGE);

        produceIntoExternalKafkaCallback.onCompletion(null, mockException);

        verify(mockException).getMessage();
        verify(mockLogger).error(String.format(ERROR_MSG, MESSAGE), mockException);
    }

}
