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
package com.expedia.www.haystack.pipes.secretDetector.actions;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class HaystackPhoneNumberFinderTest {
    private static final String [] VALID_US_PHONE_NUMBERS = {
            "1-800-555-1212", "1 (800) 555-1212", "18005551212",
            "800-555-1212", "(800) 555-1212", "8005551212",
    };

    private static final String [] INVALID_US_PHONE_NUMBERS = {
            "4640-1234-5678-9120",
    };

    private HaystackPhoneNumberFinder haystackPhoneNumberFinder;

    @Before
    public void setUp() {
        haystackPhoneNumberFinder = new HaystackPhoneNumberFinder();
    }

    @Test
    public void testGetName() {
        final String expected = HaystackPhoneNumberFinder.class.getSimpleName()
                .replace("Haystack", "")
                .replace("Finder", "");
        assertEquals(expected, haystackPhoneNumberFinder.getName());
    }

    @Test
    public void testFindStringValidNumbers() {
        for (String phoneNumber : VALID_US_PHONE_NUMBERS) {
            final List<String> strings = haystackPhoneNumberFinder.find(phoneNumber);
            assertEquals(1, strings.size());
        }
    }

    @Test
    public void testFindStringInvalidNumber() {
        final List<String> strings = haystackPhoneNumberFinder.find(("1"));
        assertEquals(0, strings.size());
    }

    @Test
    public void testFindStringsValidNumbers() {
        final List<String> phoneNumbers = Arrays.asList(VALID_US_PHONE_NUMBERS);
        final List<String> strings = haystackPhoneNumberFinder.find(phoneNumbers);
        assertEquals(VALID_US_PHONE_NUMBERS.length, strings.size());
        final Iterator<String> phoneNumbersIterator = phoneNumbers.iterator();
        final Iterator<String> stringsIterator = strings.iterator();
        while(phoneNumbersIterator.hasNext()) {
            assertEquals(phoneNumbersIterator.next(), stringsIterator.next());
        }
    }

    @Test
    public void testFindStringsInvalidNumbers() {
        final List<String> phoneNumbers = Arrays.asList(INVALID_US_PHONE_NUMBERS);
        final List<String> strings = haystackPhoneNumberFinder.find(phoneNumbers);
        assertEquals(0, strings.size());
    }
}
