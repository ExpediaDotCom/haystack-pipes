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
