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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AsynchronousFirehoseConsumerTest {
    @Mock
    private TaskExecutor mockTaskExecutor;
    @Mock
    private ApplicationContext mockApplicationContext;
    @Mock
    private FirehoseConsumer mockFirehoseConsumer;

    private AsynchronousFirehoseConsumer asynchronousFirehoseConsumer;

    @Before
    public void setUp() {
        asynchronousFirehoseConsumer = new AsynchronousFirehoseConsumer(mockTaskExecutor, mockApplicationContext);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockTaskExecutor);
        verifyNoMoreInteractions(mockApplicationContext);
        verifyNoMoreInteractions(mockFirehoseConsumer);
    }

    @Test
    public void testExecuteAsynchronously() {
        when(mockApplicationContext.getBean(FirehoseConsumer.class)).thenReturn(mockFirehoseConsumer);

        asynchronousFirehoseConsumer.executeAsynchronously();

        verify(mockApplicationContext).getBean(FirehoseConsumer.class);
        verify(mockTaskExecutor).execute(mockFirehoseConsumer);
    }
}
