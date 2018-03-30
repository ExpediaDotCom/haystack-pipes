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

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_FIELD_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_TAG_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.CREDIT_CARD_PLUS_MORE_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_IN_TAG_BYTES_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_LOG_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
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
        final List<String> secrets = detector.findSecrets(EMAIL_ADDRESS_SPAN);

        assertEquals(1, secrets.size());
        assertEquals(STRING_TAG_KEY, secrets.get(0));
    }

    @Test
    public void testFindSecretsHaystackEmailAddressInTagBytes() {
        final List<String> secrets = detector.findSecrets(EMAIL_ADDRESS_IN_TAG_BYTES_SPAN);

        assertEquals(2, secrets.size());
        assertEquals(BYTES_TAG_KEY, secrets.get(0));
        assertEquals(BYTES_FIELD_KEY, secrets.get(1));
    }

    @Test
    public void testFindSecretsHaystackEmailAddressInLog() {
        final List<String> secrets = detector.findSecrets(EMAIL_ADDRESS_LOG_SPAN);

        assertEquals(1, secrets.size());
        assertEquals(STRING_FIELD_KEY, secrets.get(0));
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
                EMAIL_ADDRESS_LOG_SPAN, Collections.singletonList(STRING_FIELD_KEY));
        assertEquals(emailText, iterator.next());
        verify(mockLogger).info(emailText);
        assertFalse(iterator.hasNext());
    }
}
