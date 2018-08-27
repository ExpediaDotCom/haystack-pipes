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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RetryCalculatorTest {
    private static final int INITIAL_RETRY_SLEEP = 10;
    private static final int MAX_RETRY_SLEEP = 9 * INITIAL_RETRY_SLEEP;

    private RetryCalculator retryCalculator;

    @Before
    public void setUp() {
        retryCalculator = new RetryCalculator(INITIAL_RETRY_SLEEP, MAX_RETRY_SLEEP);
    }

    @Test
    public void testCalculateSleepMillisAndIsTryCountAtLimit() {
        assertEquals(0, retryCalculator.calculateSleepMillis());
        assertFalse(retryCalculator.isTryCountBeyondLimit());
        assertEquals(1, retryCalculator.getTryCount());

        assertEquals(INITIAL_RETRY_SLEEP, retryCalculator.calculateSleepMillis());
        assertFalse(retryCalculator.isTryCountBeyondLimit());
        assertEquals(2, retryCalculator.getTryCount());

        assertEquals(2 * INITIAL_RETRY_SLEEP, retryCalculator.calculateSleepMillis());
        assertFalse(retryCalculator.isTryCountBeyondLimit());
        assertEquals(3, retryCalculator.getTryCount());

        assertEquals(4 * INITIAL_RETRY_SLEEP, retryCalculator.calculateSleepMillis());
        assertFalse(retryCalculator.isTryCountBeyondLimit());
        assertEquals(4, retryCalculator.getTryCount());

        assertEquals(8 * INITIAL_RETRY_SLEEP, retryCalculator.calculateSleepMillis());
        assertFalse(retryCalculator.isTryCountBeyondLimit());
        assertEquals(5, retryCalculator.getTryCount());

        assertEquals(MAX_RETRY_SLEEP, retryCalculator.calculateSleepMillis());
        assertTrue(retryCalculator.isTryCountBeyondLimit());
        assertEquals(6, retryCalculator.getTryCount());
    }

    @Test
    public void testBoundedTryCountExceedsSizeOfInteger() {
        long expected = 0;
        final int maxRetrySleep = Integer.MAX_VALUE;
        retryCalculator = new RetryCalculator(INITIAL_RETRY_SLEEP, maxRetrySleep);
        for(int i = 0 ; i < Long.SIZE ; i++) {
            final long sleepMillis = retryCalculator.calculateSleepMillis();
            assertEquals(expected, sleepMillis);
            expected = calculateExpectedForTheNextIteration(expected, maxRetrySleep);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private long calculateExpectedForTheNextIteration(long expected, int maxRetrySleep) {
        long newExpected = expected == 0 ? INITIAL_RETRY_SLEEP : expected * 2;
        if(newExpected > maxRetrySleep) {
            newExpected = maxRetrySleep;
        }
        return newExpected;
    }
}
