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

class RetryCalculator {
    final int initialRetrySleep;
    final int maxRetrySleep;
    private int boundedTryCount; // never increments more than one step past the value that causes the exponential backoff
                                 // time calculation to exceed maxRetrySleep; this avoids problems with the use of <<
                                 // during the power-of-2 exponential retry calculation for > 31 tries
    private int unboundedTryCount;

    RetryCalculator(int initialRetrySleep, int maxRetrySleep) {
        this.initialRetrySleep = initialRetrySleep;
        this.maxRetrySleep = maxRetrySleep;
    }

    /**
     * Calculates the number of milliseconds to sleep. The first time this method is called, it returns 0, so that
     * it can be called (with minimal performance impact) before the first try. The second time it returns
     * 1 * initialRetrySleep, the third time it returns 2 * initialRetrySleep, the fourth time it returns
     * 4 * initialRetrySleep, the fifth time it returns 8 * initialRetrySleep, etc. The returned value is bounded by
     * maxRetrySleep; this method will never return a number larger than maxRetrySleep.
     *
     * @return milliseconds to sleep
     */
    int calculateSleepMillis() {
        final int sleepMillisPerTryCount = (1 << (boundedTryCount - 1)) * initialRetrySleep;
        final int sleepMillis;
        if (sleepMillisPerTryCount > maxRetrySleep) {
            sleepMillis = maxRetrySleep;
        } else {
            sleepMillis = sleepMillisPerTryCount;
            ++boundedTryCount;
        }
        ++unboundedTryCount;
        return Math.min(sleepMillis, maxRetrySleep);
    }

    boolean isTryCountBeyondLimit() {
        return unboundedTryCount > boundedTryCount;
    }

    int getUnboundedTryCount() {
        return unboundedTryCount;
    }
}
