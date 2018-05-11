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

import com.expedia.www.haystack.commons.config.Configuration;
import com.expedia.www.haystack.pipes.secretDetector.actions.DetectedAction;
import com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction;
import org.cfg4j.provider.ConfigurationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import java.util.Collections;
import java.util.List;

import static com.expedia.www.haystack.pipes.secretDetector.config.ActionsConfigurationProvider.HAYSTACK_SECRETS_CONFIG_PREFIX;
import static com.expedia.www.haystack.pipes.secretDetector.config.ActionsConfigurationProvider.PROBLEM_USING_CONFIGURATION;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ActionsConfigurationProviderTest {
    private static final String NON_EXISTENT_BEAN_NAME = "nonExistentBeanName";

    @Mock
    private Logger mockLogger;
    @Mock
    private ActionsConfig mockActionsConfig;

    private ActionsConfigurationProvider actionsConfigurationProvider;

    @Before
    public void setUp() {
        final Configuration configuration = new Configuration();
        final ConfigurationProvider mergeConfigurationProvider = configuration.createMergeConfigurationProvider();
        actionsConfigurationProvider = new ActionsConfigurationProvider(mockLogger, mergeConfigurationProvider);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockLogger, mockActionsConfig);
    }

    @Test
    public void testGetDetectedActionsHappyCase() {
        final List<DetectedAction> detectedActions = actionsConfigurationProvider.getDetectedActions();

        assertEquals(1, detectedActions.size());
        final DetectedAction detectedAction = detectedActions.get(0);
        assertEquals(EmailerDetectedAction.class, detectedAction.getClass());
    }

    @Test(expected = RuntimeException.class)
    public void testGetDetectedActionsExceptionCase() {
        actionsConfigurationProvider = new ActionsConfigurationProvider(mockLogger, mockActionsConfig);
        when(mockActionsConfig.actionfactories()).thenReturn(Collections.singletonList(NON_EXISTENT_BEAN_NAME));
        try {
            actionsConfigurationProvider.getDetectedActions();
        } catch(RuntimeException e) {
            verify(mockActionsConfig).actionfactories();
            final String configurationName = HAYSTACK_SECRETS_CONFIG_PREFIX + ".actionfactories";
            final String logMsg = String.format(PROBLEM_USING_CONFIGURATION, configurationName, NON_EXISTENT_BEAN_NAME);
            verify(mockLogger).error(logMsg);
            final String exceptionMsg = String.format("%s: No bean named '%s' available",
                    NoSuchBeanDefinitionException.class.getName(), NON_EXISTENT_BEAN_NAME);
            assertEquals(exceptionMsg, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testMainBean() {
        assertEquals("protobufToDetectorAction", actionsConfigurationProvider.mainbean());
    }
}
