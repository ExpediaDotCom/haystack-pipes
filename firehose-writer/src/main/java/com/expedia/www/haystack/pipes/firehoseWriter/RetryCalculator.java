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

import java.math.BigInteger;

class RetryCalculator {
    final BigInteger initialRetrySleep;
    final BigInteger maxRetrySleep;
    private BigInteger tryCount;
    private boolean isTryCountBeyondLimit;

    RetryCalculator(int initialRetrySleep, int maxRetrySleep) {
        this.initialRetrySleep = BigInteger.valueOf(initialRetrySleep);
        this.maxRetrySleep = BigInteger.valueOf(maxRetrySleep);
        tryCount = BigInteger.ZERO;
        isTryCountBeyondLimit = false;
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
        // min(1 << (tryCount - 1)) * initialRetrySleep), maxRetrySleep);
        final BigInteger sleepMillisBeforeMin = BigInteger.ONE
                .shiftLeft(tryCount.subtract(BigInteger.ONE).intValue())
                .multiply(initialRetrySleep);
        isTryCountBeyondLimit = sleepMillisBeforeMin.compareTo(maxRetrySleep) >= 0;
        tryCount = tryCount.add(BigInteger.ONE);
        return tryCount.equals(BigInteger.ONE) ? 0 : sleepMillisBeforeMin.min(maxRetrySleep).intValue();
    }

    boolean isTryCountBeyondLimit() {
        return isTryCountBeyondLimit;
    }

    int getTryCount() {
        return tryCount.intValue();
    }
}
