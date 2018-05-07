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
package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.commons.secretDetector.NonLocalIpV4AddressFinder;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.secretDetector.Detector.Factory;
import com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction;
import com.netflix.servo.monitor.Counter;
import io.dataapps.chlorine.finder.FinderEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_FIELD_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_TAG_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.CREDIT_CARD_LOG_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_IN_TAG_BYTES_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_LOG_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.IP_ADDRESS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.IP_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.OPERATION_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.SERVICE_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_FIELD_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_FIELD_VALUE;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_TAG_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.buildSpan;
import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;
import static com.expedia.www.haystack.pipes.secretDetector.Detector.COUNTER_NAME;
import static com.expedia.www.haystack.pipes.secretDetector.Detector.ERRORS_METRIC_GROUP;
import static com.expedia.www.haystack.pipes.secretDetector.Detector.FINDERS_TO_LOG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DetectorTest {
    private static final String FINDER_NAME = RANDOM.nextLong() + "FINDER_NAME";
    private static final FinderEngine FINDER_ENGINE = new FinderEngine();
    private static final String CREDIT_CARD_FINDER_NAME = "Credit_Card";
    private static final String CREDIT_CARD_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML = CREDIT_CARD_FINDER_NAME;
    private static final String EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML = "Email";
    private static final String IP_FINDER_NAME = NonLocalIpV4AddressFinder.class.getSimpleName().replace("Finder", "");
    private static final FinderNameAndServiceName FINDER_NAME_AND_SERVICE_NAME
            = new FinderNameAndServiceName(FINDER_NAME, SERVICE_NAME);

    @Mock
    private Logger mockLogger;

    @Mock
    private Factory mockFactory;

    @Mock
    private Counter mockCounter;

    @Mock
    private MetricObjects mockMetricObjects;

    @Mock
    private S3ConfigFetcher mockS3ConfigFetcher;

    private Detector detector;
    private Factory factory;

    @Before
    public void setUp() {
        S3ConfigFetcher.WHITE_LIST_ITEMS.set(new ConcurrentHashMap<>());
        detector = new Detector(mockLogger, FINDER_ENGINE, mockFactory, mockS3ConfigFetcher);
        factory = new Factory(mockMetricObjects);
    }

    @After
    public void tearDown() {
        Detector.COUNTERS.clear();
        verifyNoMoreInteractions(mockLogger, mockFactory, mockCounter, mockMetricObjects, mockS3ConfigFetcher);
    }

    @Test
    public void testFindSecretsHaystackEmailAddress() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        final Map<String, List<String>> secrets = detector.findSecrets(EMAIL_ADDRESS_SPAN);
        verifyHaystackEmailAddressFound(secrets, STRING_TAG_KEY);
    }

    @Test
    public void testFindSecretsHaystackEmailAddressInTagBytes() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        final Map<String, List<String>> secrets = detector.findSecrets(EMAIL_ADDRESS_IN_TAG_BYTES_SPAN);

        assertEquals(1, secrets.size());
        final List<String> tagsThatContainEmails = secrets.get(EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML);
        assertEquals(2, tagsThatContainEmails.size());
        final Iterator<String> iterator = tagsThatContainEmails.iterator();
        assertEquals(BYTES_TAG_KEY, iterator.next());
        assertEquals(BYTES_FIELD_KEY, iterator.next());
    }

    @Test
    public void testFindSecretsHaystackEmailAddressInLog() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        final Map<String, List<String>> secrets = detector.findSecrets(EMAIL_ADDRESS_LOG_SPAN);

        verifyHaystackEmailAddressFound(secrets, STRING_FIELD_KEY);
    }

    private void verifyHaystackEmailAddressFound(Map<String, List<String>> secrets, String keyOfSecret) {
        assertEquals(1, secrets.size());
        assertEquals(EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML, secrets.keySet().iterator().next());
        final List<String> strings = secrets.get(EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML);
        assertEquals(1, strings.size());
        assertEquals(keyOfSecret, strings.iterator().next());
    }

    @Test
    public void testFindSecretsIpAddress() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        final Map<String, List<String>> secrets = detector.findSecrets(IP_ADDRESS_SPAN);

        assertEquals(IP_ADDRESS + " should have been flagged as a secret", 1, secrets.size());
        assertEquals(IP_FINDER_NAME, secrets.keySet().iterator().next());
        assertEquals(STRING_TAG_KEY, secrets.get(IP_FINDER_NAME).iterator().next());
    }

    @Test
    public void testFindSecretsNoSecret() {
        assertTrue(detector.findSecrets(FULLY_POPULATED_SPAN).isEmpty());
    }

    @Test
    public void testFindSecretsSocialSecurityNumberFalsePositive() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        final String stringWithFalsePositiveSsn = "-250-14-9479-1-";
        final String jsonWithFalsePositiveSsn = JSON_SPAN_STRING.replace(STRING_FIELD_VALUE, stringWithFalsePositiveSsn);
        final Span span = buildSpan(jsonWithFalsePositiveSsn);
        final Map<String, List<String>> secrets = detector.findSecrets(span);
        assertTrue(secrets.isEmpty());
    }

    @Test
    public void testApplyNoSecret() {
        final Iterable<String> iterable = detector.apply(FULLY_POPULATED_SPAN);
        assertFalse(iterable.iterator().hasNext());
    }

    @Test
    public void testApplyCreditCardInLog() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        when(mockS3ConfigFetcher.isTagInWhiteList(anyString(), anyString(), anyString(), anyString())).thenReturn(false);
        final Iterator<String> iterator = detector.apply(CREDIT_CARD_LOG_SPAN).iterator();

        final String emailText = EmailerDetectedAction.getEmailText(
                CREDIT_CARD_LOG_SPAN, Collections.singletonMap(CREDIT_CARD_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML,
                        Collections.singletonList(STRING_FIELD_KEY)));
        assertEquals(emailText, iterator.next());
        assertFalse(iterator.hasNext());
        verify(mockS3ConfigFetcher).isTagInWhiteList(
                CREDIT_CARD_FINDER_NAME, SERVICE_NAME, OPERATION_NAME, STRING_FIELD_KEY);
        if(FINDERS_TO_LOG.contains(CREDIT_CARD_FINDER_NAME)) {
            verify(mockLogger).info(emailText);
        }
        verifyCounterIncrement();
    }

    @Test
    public void testApplyEMailAddressInLog() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        when(mockS3ConfigFetcher.isTagInWhiteList(anyString(), anyString(), anyString(), anyString())).thenReturn(true, false);
        for (int i = 0 ; i < 2 ; i++) {
            final Iterator<String> iterator = detector.apply(EMAIL_ADDRESS_LOG_SPAN).iterator();
            final String emailText = EmailerDetectedAction.getEmailText(
                    EMAIL_ADDRESS_LOG_SPAN, Collections.singletonMap(
                            EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML, Collections.singletonList(STRING_FIELD_KEY)));
            assertEquals(emailText, iterator.next());
            assertFalse(iterator.hasNext());
        }
        verify(mockS3ConfigFetcher, times(2)).isTagInWhiteList(
                "Email", SERVICE_NAME, OPERATION_NAME, STRING_FIELD_KEY);
    }

    private void verifyCounterIncrement() {
        final FinderNameAndServiceName finderAndServiceName =
                new FinderNameAndServiceName(CREDIT_CARD_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML, SERVICE_NAME);
        verify(mockFactory).createCounter(finderAndServiceName);
        verify(mockCounter, times(1)).increment();
    }

    @Test
    public void testFactoryCreateCounter() {
        when(mockMetricObjects.createAndRegisterResettingCounter(
                anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(mockCounter);

        final Counter counter = factory.createCounter(FINDER_NAME_AND_SERVICE_NAME);

        assertSame(mockCounter, counter);
        verify(mockMetricObjects).createAndRegisterResettingCounter(
                ERRORS_METRIC_GROUP, APPLICATION, FINDER_NAME, SERVICE_NAME, COUNTER_NAME);
    }

}
