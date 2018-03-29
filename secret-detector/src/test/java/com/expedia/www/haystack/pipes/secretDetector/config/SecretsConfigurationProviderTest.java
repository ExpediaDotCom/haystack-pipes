package com.expedia.www.haystack.pipes.secretDetector.config;

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
    public void testFrom() {
        assertEquals("haystack@expedia.com", secretsConfigurationProvider.from());
    }

    @Test
    public void testTos() {
        final List<String> expected = Arrays.asList("haystack@expedia.com", "test@expedia.com");

        assertEquals(expected, secretsConfigurationProvider.tos());
    }

    @Test
    public void testHost() {
        assertEquals("localhost", secretsConfigurationProvider.host());
    }

    @Test
    public void testSubject() {
        assertEquals("[Action Required] Security alert in haystack spans!", secretsConfigurationProvider.subject());
    }
}
