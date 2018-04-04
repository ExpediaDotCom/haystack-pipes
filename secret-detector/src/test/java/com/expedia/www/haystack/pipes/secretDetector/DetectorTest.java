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

import com.expedia.www.haystack.metrics.MetricObjects;
import com.expedia.www.haystack.pipes.secretDetector.Detector.Factory;
import com.expedia.www.haystack.pipes.secretDetector.Detector.FinderNameAndServiceName;
import com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction;
import com.expedia.www.haystack.pipes.secretDetector.actions.NonLocalIpV4AddressFinder;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_FIELD_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_TAG_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.CREDIT_CARD_LOG_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_IN_TAG_BYTES_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_LOG_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.IP_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.SERVICE_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_FIELD_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_TAG_KEY;
import static com.expedia.www.haystack.pipes.secretDetector.Constants.APPLICATION;
import static com.expedia.www.haystack.pipes.secretDetector.Detector.COUNTER_NAME;
import static com.expedia.www.haystack.pipes.secretDetector.Detector.ERRORS_METRIC_GROUP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
    private static final String CREDIT_CARD_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML = "Credit_Card";
    private static final String EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML = "Email";
    private static final String IP_FINDER_NAME = NonLocalIpV4AddressFinder.class.getSimpleName();
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

    private Detector detector;
    private Factory factory;

    @Before
    public void setUp() {
        detector = new Detector(mockLogger, FINDER_ENGINE, mockFactory);
        factory = new Factory(mockMetricObjects);
    }

    @After
    public void tearDown() {
        Detector.COUNTERS = Collections.synchronizedMap(new HashMap<>());
        verifyNoMoreInteractions(mockLogger, mockFactory, mockCounter, mockMetricObjects);
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
        verifyCounterIncrement(2, EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML);
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
        verifyCounterIncrement(1, EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML);
    }

    @Test
    public void testFindSecretsIpAddress() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        final Map<String, List<String>> secrets = detector.findSecrets(IP_ADDRESS_SPAN);

        assertEquals(1, secrets.size());
        assertEquals(IP_FINDER_NAME, secrets.keySet().iterator().next());
        assertEquals(STRING_TAG_KEY, secrets.get(IP_FINDER_NAME).iterator().next());
        verifyCounterIncrement(1, IP_FINDER_NAME);
    }

    @Test
    public void testFindSecretsNoSecret() {
        assertTrue(detector.findSecrets(FULLY_POPULATED_SPAN).isEmpty());
    }

    @Test
    public void testApplyNoSecret() {
        final Iterable<String> iterable = detector.apply(FULLY_POPULATED_SPAN);
        assertFalse(iterable.iterator().hasNext());
    }

    @Test
    public void testApplyCreditCardInLog() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        final Iterator<String> iterator = detector.apply(CREDIT_CARD_LOG_SPAN).iterator();

        final String emailText = EmailerDetectedAction.getEmailText(
                CREDIT_CARD_LOG_SPAN, Collections.singletonMap(CREDIT_CARD_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML,
                        Collections.singletonList(STRING_FIELD_KEY)));
        assertEquals(emailText, iterator.next());
        assertFalse(iterator.hasNext());
        verify(mockLogger).info(emailText);
        verifyCounterIncrement(1, CREDIT_CARD_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML);
    }

    @Test
    public void testApplyEMailAddressInLog() {
        when(mockFactory.createCounter(any())).thenReturn(mockCounter);
        final Iterator<String> iterator = detector.apply(EMAIL_ADDRESS_LOG_SPAN).iterator();

        final String emailText = EmailerDetectedAction.getEmailText(
                EMAIL_ADDRESS_LOG_SPAN, Collections.singletonMap(
                        EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML, Collections.singletonList(STRING_FIELD_KEY)));
        assertEquals(emailText, iterator.next());
        assertFalse(iterator.hasNext());
        verifyCounterIncrement(1, EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML);
    }

    private void verifyCounterIncrement(int wantedNumberOfInvocations, String finderName) {
        final FinderNameAndServiceName finderAndServiceName =
                new FinderNameAndServiceName(finderName, SERVICE_NAME);
        verify(mockFactory).createCounter(finderAndServiceName);
        verify(mockCounter, times(wantedNumberOfInvocations)).increment();
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

    @Test
    public void testFinderNameAndServiceNameEqualsNullOther() {
        //noinspection SimplifiableJUnitAssertion,ConstantConditions,ObjectEqualsNull
        assertFalse(FINDER_NAME_AND_SERVICE_NAME.equals(null));
    }

    @Test
    public void testFinderNameAndServiceNameEqualsSameOther() {
        assertEquals(FINDER_NAME_AND_SERVICE_NAME, FINDER_NAME_AND_SERVICE_NAME);
    }

    @Test
    public void testFinderNameAndServiceNameEqualsDifferentClassOther() {
        //noinspection SimplifiableJUnitAssertion,EqualsBetweenInconvertibleTypes
        assertFalse(FINDER_NAME_AND_SERVICE_NAME.equals(FINDER_NAME));
    }

    @Test
    public void testFinderNameAndServiceNameEqualsTotalMatch() {
        final FinderNameAndServiceName finderNameAndServiceName
                = new FinderNameAndServiceName(FINDER_NAME, SERVICE_NAME);
        assertEquals(FINDER_NAME_AND_SERVICE_NAME, finderNameAndServiceName);
    }

    @Test
    public void testFinderNameAndServiceNameEqualsFinderNameMisMatch() {
        final FinderNameAndServiceName finderNameAndServiceName13
                = new FinderNameAndServiceName("1", "3");
        final FinderNameAndServiceName finderNameAndServiceName23
                = new FinderNameAndServiceName("2", "3");
        assertNotEquals(finderNameAndServiceName13, finderNameAndServiceName23);
    }

    @Test
    public void testFinderNameAndServiceNameEqualsServiceNameMisMatch() {
        final FinderNameAndServiceName finderNameAndServiceName12
                = new FinderNameAndServiceName("1", "2");
        final FinderNameAndServiceName finderNameAndServiceName13
                = new FinderNameAndServiceName("1", "3");
        assertNotEquals(finderNameAndServiceName12, finderNameAndServiceName13);
    }

    @Test
    public void testFinderNameAndServiceNameHashCodeFinderNameMisMatch() {
        final FinderNameAndServiceName finderNameAndServiceName13
                = new FinderNameAndServiceName("1", "3");
        final FinderNameAndServiceName finderNameAndServiceName23
                = new FinderNameAndServiceName("2", "3");
        assertNotEquals(finderNameAndServiceName13.hashCode(), finderNameAndServiceName23.hashCode());
    }

    @Test
    public void testFinderNameAndServiceNameHashCodeServiceNameMisMatch() {
        final FinderNameAndServiceName finderNameAndServiceName12
                = new FinderNameAndServiceName("1", "2");
        final FinderNameAndServiceName finderNameAndServiceName13
                = new FinderNameAndServiceName("1", "3");
        assertNotEquals(finderNameAndServiceName12.hashCode(), finderNameAndServiceName13.hashCode());
    }

}
