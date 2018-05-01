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

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class WhiteListItemTest {
    private static final String FINDER_NAME = RANDOM.nextLong() + "FINDER_NAME";
    private static final String SERVICE_NAME = RANDOM.nextLong() + "SERVICE_NAME";
    private static final String OPERATION_NAME = RANDOM.nextLong() + "OPERATION_NAME";
    private static final String TAG_NAME = RANDOM.nextLong() + "TAG_NAME";
    private WhiteListItem whiteListItem;

    @Before
    public void setUp() {
        whiteListItem = new WhiteListItem(FINDER_NAME, SERVICE_NAME, OPERATION_NAME, TAG_NAME);
    }

    @Test
    public void testEqualsSameObject() {
        assertEquals(whiteListItem, whiteListItem);
    }

    @Test
    public void testEqualsNullOther() {
        assertNotEquals(whiteListItem, null);
    }

    @Test
    public void testEqualsWrongClass() {
        assertNotEquals(whiteListItem, "This string is not an instance of WhiteListItem");
    }

    @Test
    public void testEqualsAndHashCodeSameAttributes() {
        final WhiteListItem other = new WhiteListItem(FINDER_NAME, SERVICE_NAME, OPERATION_NAME, TAG_NAME);

        assertEquals(whiteListItem, other);
        assertEquals(whiteListItem.hashCode(), other.hashCode());
    }

    @Test
    public void testEqualsAndHashCodeNullFinderName() {
        final WhiteListItem other = new WhiteListItem(null, SERVICE_NAME, OPERATION_NAME, TAG_NAME);

        assertNotEquals(whiteListItem, other);
        assertNotEquals(whiteListItem.hashCode(), other.hashCode());
    }

    @Test
    public void testEqualsAndHashCodeNullServiceName() {
        final WhiteListItem other = new WhiteListItem(FINDER_NAME, null, OPERATION_NAME, TAG_NAME);

        assertNotEquals(whiteListItem, other);
        assertNotEquals(whiteListItem.hashCode(), other.hashCode());
    }

    @Test
    public void testEqualsAndHashCodeNullOperationName() {
        final WhiteListItem other = new WhiteListItem(FINDER_NAME, SERVICE_NAME, null, TAG_NAME);

        assertNotEquals(whiteListItem, other);
        assertNotEquals(whiteListItem.hashCode(), other.hashCode());
    }

    @Test
    public void testEqualsAndHashCodeNullTagName() {
        final WhiteListItem other = new WhiteListItem(FINDER_NAME, SERVICE_NAME, OPERATION_NAME, null);

        assertNotEquals(whiteListItem, other);
        assertNotEquals(whiteListItem.hashCode(), other.hashCode());
    }
}
