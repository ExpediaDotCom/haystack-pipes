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
