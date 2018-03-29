package com.expedia.www.haystack.pipes.secretDetector;

import io.dataapps.chlorine.finder.FinderEngine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_FIELD_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.BYTES_TAG_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_IN_TAG_BYTES_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_LOG_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.EMAIL_ADDRESS_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_FIELD_KEY;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.STRING_TAG_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class DetectorTest {
    private static final FinderEngine FINDER_ENGINE = new FinderEngine();

    private Detector detector;

    @Before
    public void setUp() {
        detector = new Detector(FINDER_ENGINE);
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
}
