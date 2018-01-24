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

import com.netflix.servo.monitor.Counter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Random;

import static com.expedia.www.haystack.pipes.firehoseWriter.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class FirehoseActionTest {
    private static final Random RANDOM = new Random();
    private static final String KEY = RANDOM.nextLong() + "KEY";

    @Mock
    private Counter mockCounter;

    private FirehoseAction firehoseAction;

    @Before
    public void setUp() {
        firehoseAction = new FirehoseAction(mockCounter);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockCounter);
    }

    @Test
    public void testApply() {
        firehoseAction.apply(KEY, FULLY_POPULATED_SPAN);

        verify(mockCounter).increment();
    }
}
