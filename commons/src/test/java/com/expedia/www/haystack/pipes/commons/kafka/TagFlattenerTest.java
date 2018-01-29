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
package com.expedia.www.haystack.pipes.commons.kafka;

import org.junit.Before;
import org.junit.Test;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_BOGUS_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_EMPTY_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_FLATTENED_TAGS;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.JSON_SPAN_STRING_WITH_NO_TAGS;
import static org.junit.Assert.assertEquals;

public class TagFlattenerTest {
    private TagFlattener tagFlattener;

    @Before
    public void setUp() {
        tagFlattener = new TagFlattener();
    }

    @Test
    public void testFlattenTags() {
        final String flattenedTags = tagFlattener.flattenTags(JSON_SPAN_STRING);

        assertEquals(JSON_SPAN_STRING_WITH_FLATTENED_TAGS, flattenedTags);
    }

    @Test
    public void testFlattenTagsWithEmptyTags() {
        final String flattenedTags = tagFlattener.flattenTags(JSON_SPAN_STRING_WITH_EMPTY_TAGS);

        assertEquals(JSON_SPAN_STRING_WITH_EMPTY_TAGS, flattenedTags);
    }

    @Test
    public void testFlattenTagsWithBogusTag() {
        final String flattenedTags = tagFlattener.flattenTags(JSON_SPAN_STRING_WITH_BOGUS_TAGS);

        assertEquals(JSON_SPAN_STRING_WITH_EMPTY_TAGS, flattenedTags);
    }

    @Test
    public void testFlattenTagsWithoutTags() {
        final String flattenedTags = tagFlattener.flattenTags(JSON_SPAN_STRING_WITH_NO_TAGS);

        assertEquals(JSON_SPAN_STRING_WITH_NO_TAGS, flattenedTags);
    }


}
