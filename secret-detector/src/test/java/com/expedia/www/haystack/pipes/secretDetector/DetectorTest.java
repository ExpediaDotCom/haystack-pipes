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

import com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction;
import com.expedia.www.haystack.pipes.secretDetector.actions.NonLocalIpV4AddressFinder;
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

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_FIELD_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_TAG_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.CREDIT_CARD_PLUS_MORE_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_IN_TAG_BYTES_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_LOG_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.IP_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_FIELD_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_TAG_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class DetectorTest {
    private static final FinderEngine FINDER_ENGINE = new FinderEngine();
    private static final String EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML = "Email";

    @Mock
    private Logger mockLogger;

    private Detector detector;

    @Before
    public void setUp() {
        detector = new Detector(mockLogger, FINDER_ENGINE);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockLogger);
    }

    @Test
    public void testFindSecretsHaystackEmailAddress() {
        final Map<String, List<String>> secrets = detector.findSecrets(EMAIL_ADDRESS_SPAN);

        verifyHaystackEmailAddressFound(secrets, STRING_TAG_KEY);
    }

    @Test
    public void testFindSecretsHaystackEmailAddressInTagBytes() {
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
        final Map<String, List<String>> secrets = detector.findSecrets(IP_ADDRESS_SPAN);

        assertEquals(1, secrets.size());
        final String finderName = NonLocalIpV4AddressFinder.class.getSimpleName();
        assertEquals(finderName, secrets.keySet().iterator().next());
        assertEquals(STRING_TAG_KEY, secrets.get(finderName).iterator().next());
    }

    @Test
    public void testFindSecretsNoSecret() {
        assertTrue(detector.findSecrets(FULLY_POPULATED_SPAN).isEmpty());
    }

    @Test
    public void testFindSecretsCreditCardPlusMore() {
        assertTrue(detector.findSecrets(CREDIT_CARD_PLUS_MORE_SPAN).isEmpty());
    }

    @Test
    public void testApplyNoSecret() {
        final Iterable<String> iterable = detector.apply(FULLY_POPULATED_SPAN);
        assertFalse(iterable.iterator().hasNext());
    }

    @Test
    public void testApplyEMailAddressInLog() {
        final Iterator<String> iterator = detector.apply(EMAIL_ADDRESS_LOG_SPAN).iterator();

        final String emailText = EmailerDetectedAction.getEmailText(
                EMAIL_ADDRESS_LOG_SPAN, Collections.singletonMap(
                        EMAIL_FINDER_NAME_IN_FINDERS_DEFAULT_DOT_XML, Collections.singletonList(STRING_FIELD_KEY)));
        assertEquals(emailText, iterator.next());
        verify(mockLogger).info(emailText);
        assertFalse(iterator.hasNext());
    }
}
