package com.expedia.www.haystack.pipes.secretDetector;

import io.dataapps.chlorine.finder.FinderEngine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_IN_TAG_BYTES_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_LOG_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.IP_ADDRESS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.IP_ADDRESS_SPAN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class DetectorTest {
    private final static FinderEngine FINDER_ENGINE = new FinderEngine();

    private Detector detector;

    @Before
    public void setUp() {
        detector = new Detector(FINDER_ENGINE);
    }

    @Test
    public void testFindSecretsHaystackEmailAddress() {
        final List<String> secrets = detector.findSecrets(EMAIL_ADDRESS_SPAN);

        assertEquals(1, secrets.size());
        final String secret = secrets.get(0);
        assertEquals(EMAIL_ADDRESS, secret);
    }

    @Test
    public void testFindSecretsHaystackEmailAddressInTagBytes() {
        final List<String> secrets = detector.findSecrets(EMAIL_ADDRESS_IN_TAG_BYTES_SPAN);

        assertEquals(2, secrets.size());
        for(final String secret :secrets) {
            assertEquals(EMAIL_ADDRESS, secret);
        }
    }

    @Test
    public void testFindSecretsHaystackEmailAddressInLog() {
        final List<String> secrets = detector.findSecrets(EMAIL_ADDRESS_LOG_SPAN);

        assertEquals(1, secrets.size());
        final String secret = secrets.get(0);
        assertEquals(EMAIL_ADDRESS, secret);
    }

    @Test
    public void testFindSecretsIpAddress() {
        final List<String> secrets = detector.findSecrets(IP_ADDRESS_SPAN);

        assertEquals(1, secrets.size());
        final String secret = secrets.get(0);
        assertEquals(IP_ADDRESS, secret);
    }

    @Test
    public void testFindSecretsNoSecret() {
        assertTrue(detector.findSecrets(FULLY_POPULATED_SPAN).isEmpty());
    }
}
