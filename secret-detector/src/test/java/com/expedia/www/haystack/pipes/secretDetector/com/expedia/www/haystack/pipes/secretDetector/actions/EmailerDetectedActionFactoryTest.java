package com.expedia.www.haystack.pipes.secretDetector.com.expedia.www.haystack.pipes.secretDetector.actions;

import com.expedia.www.haystack.pipes.secretDetector.SecretsEmailConfigurationProvider;
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
public class EmailerDetectedActionFactoryTest {
    @Mock
    private EmailerDetectedAction.Factory mockEmailerFactory;
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
                mockEmailerFactory, mockEmailerLogger, mockSender, mockSecretsEmailConfigurationProvider);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockEmailerFactory, mockEmailerLogger, mockSender, mockSecretsEmailConfigurationProvider);
    }

    @Test
    public void testCreate() {
        EmailerDetectedActionTest.whensForConstructor(mockSecretsEmailConfigurationProvider);

        assertNotNull(emailerDetectedActionFactory.create());

        EmailerDetectedActionTest.verifiesForConstructor(mockSecretsEmailConfigurationProvider, 1);
    }
}
