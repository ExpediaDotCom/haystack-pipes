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

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.dataapps.chlorine.finder.Finder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Component
public class HaystackPhoneNumberFinder implements Finder {
    public static final String FINDER_NAME = "PhoneNumber";
    private final PhoneNumberUtil phoneNumberUtil;

    @Autowired
    public HaystackPhoneNumberFinder(PhoneNumberUtil phoneNumberUtil) {
        this.phoneNumberUtil = phoneNumberUtil;
    }

    @Override
    public String getName() {
        return FINDER_NAME;
    }

    @Override
    public List<String> find(Collection<String> inputs) {
        final List<String> list = new ArrayList<>();
        for(String input : inputs) {
            list.addAll(find(input));
        }
        return list;
    }

    @Override
    public List<String> find(String input) {
        try {
            final Phonenumber.PhoneNumber phoneNumber = phoneNumberUtil.parseAndKeepRawInput(input, "US");
            return Collections.singletonList(phoneNumber.getRawInput());
        } catch (NumberParseException e) {
            // Just ignore, it's not a phone number
        }
        return Collections.emptyList();
    }
}
