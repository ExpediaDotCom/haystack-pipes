package com.expedia.www.haystack.pipes.secretDetector;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SecretsConfigurationProviderTest {
    private SecretsConfigurationProvider secretsConfigurationProvider;

    @Before
    public void setUp() {
        secretsConfigurationProvider = new SecretsConfigurationProvider();
    }

    @Test
    public void testEmails() {
        final List<String> expected = Arrays.asList("haystack@expedia.com", "test@expedia.com");

        assertEquals(expected, secretsConfigurationProvider.emails());
    }
}
