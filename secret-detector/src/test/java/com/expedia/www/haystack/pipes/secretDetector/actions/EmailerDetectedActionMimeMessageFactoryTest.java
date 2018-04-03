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

import com.expedia.www.haystack.pipes.secretDetector.config.SecretsEmailConfigurationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class EmailerDetectedActionMimeMessageFactoryTest {
    @Mock
    private EmailerDetectedAction.MimeMessageFactory mockEmailerMimeMessageFactory;
    @Mock
    private Logger mockEmailerLogger;
    @Mock
    private EmailerDetectedAction.Sender mockSender;
    @Mock
    private SecretsEmailConfigurationProvider mockSecretsEmailConfigurationProvider;

    private EmailerDetectedActionFactory emailerDetectedActionFactory;

    @Before
    public void setUp() {
        emailerDetectedActionFactory = new EmailerDetectedActionFactory(
                mockEmailerMimeMessageFactory, mockEmailerLogger, mockSender, mockSecretsEmailConfigurationProvider);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockEmailerMimeMessageFactory, mockEmailerLogger, mockSender, mockSecretsEmailConfigurationProvider);
    }

    @Test
    public void testCreate() {
        EmailerDetectedActionTest.whensForConstructor(mockSecretsEmailConfigurationProvider);

        assertNotNull(emailerDetectedActionFactory.create());

        EmailerDetectedActionTest.verifiesForConstructor(mockSecretsEmailConfigurationProvider, 1);
    }
}
