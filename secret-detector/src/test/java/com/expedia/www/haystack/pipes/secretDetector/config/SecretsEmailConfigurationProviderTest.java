package com.expedia.www.haystack.pipes.secretDetector.config;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SecretsEmailConfigurationProviderTest {
    private SecretsEmailConfigurationProvider secretsEmailConfigurationProvider;

    @Before
    public void setUp() {
        secretsEmailConfigurationProvider = new SecretsEmailConfigurationProvider();
    }

    @Test
    public void testFrom() {
        assertEquals("haystack@expedia.com", secretsEmailConfigurationProvider.from());
    }

    @Test
    public void testTos() {
        final List<String> expected = Arrays.asList("haystack@expedia.com", "test@expedia.com");

        assertEquals(expected, secretsEmailConfigurationProvider.tos());
    }

    @Test
    public void testHost() {
        assertEquals("localhost", secretsEmailConfigurationProvider.host());
    }

    @Test
    public void testSubject() {
        assertEquals("[Action Required] Security alert in haystack spans!", secretsEmailConfigurationProvider.subject());
    }
}
