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
package com.expedia.www.haystack.pipes.firehoseWriter;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FirehoseConfigurationProviderTest {
    private FirehoseConfigurationProvider firehoseConfigurationProvider;

    @Before
    public void setUp() {
        firehoseConfigurationProvider = new FirehoseConfigurationProvider();
    }

    @Test
    public void testRetryCount() {
        assertEquals("3", firehoseConfigurationProvider.retrycount());
    }

    @Test
    public void testUrl() {
        assertEquals("https://firehose.us-west-2.amazonaws.com", firehoseConfigurationProvider.url());
    }

    @Test
    public void testStreamName() {
        assertEquals("haystack-traces-test", firehoseConfigurationProvider.streamname());
    }
}
