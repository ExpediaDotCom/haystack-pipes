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
import com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction.MimeMessageFactory;
import com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction.Sender;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import javax.mail.Address;
import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.OPERATION_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.SERVICE_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.SPAN_ID;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.TRACE_ID;
import static com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction.HOST_KEY;
import static com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction.SENDING_EXCEPTION_MSG;
import static com.expedia.www.haystack.pipes.secretDetector.actions.EmailerDetectedAction.TEXT_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EmailerDetectedActionTest {
    private static final String FROM = RANDOM.nextInt(Integer.MAX_VALUE) + "@expedia.com";
    private static final String TO1 = RANDOM.nextInt(Integer.MAX_VALUE) + "@expedia.com";
    private static final String TO2 = RANDOM.nextInt(Integer.MAX_VALUE) + "@expedia.com";
    private static final List<String> TOS = ImmutableList.of(TO1, TO2);
    private static final Address[] TO_ADDRESSES = new Address[TOS.size()];
    private static final String ILLEGAL_ADDRESS = "Illegal address";

    static {
        for(int i = 0; i < TOS.size() ; i++) {
            try {
                TO_ADDRESSES[i] = new InternetAddress(TOS.get(i));
            } catch (AddressException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private static final String SUBJECT = RANDOM.nextLong() + "SUBJECT";
    private static final String HOST = "localhost"; //RANDOM.nextLong() + "HOST";
    private static final String KEY = RANDOM.nextLong() + "KEY";
    private static final String SECRET = RANDOM.nextLong() + "SECRET";
    private static final Map<String, List<String>> SECRETS = ImmutableMap.of(KEY, Collections.singletonList(SECRET));
    private static final String EMAIL_TEXT =
            String.format(TEXT_TEMPLATE, SERVICE_NAME, OPERATION_NAME, SPAN_ID, TRACE_ID, SECRETS);

    @Mock
    private MimeMessageFactory mockMimeMessageFactory;
    @Mock
    private Logger mockLogger;
    @Mock
    private Sender mockSender;
    @Mock
    private SecretsEmailConfigurationProvider mockSecretsEmailConfigurationProvider;
    @Mock
    private MimeMessage mockMimeMessage;
    @Mock
    private FromAddressExceptionLogger mockFromAddressExceptionLogger;
    @Mock
    private ToAddressExceptionLogger mockToAddressExceptionLogger;

    private EmailerDetectedAction emailerDetectedAction;
    private MimeMessageFactory mimeMessageFactory;
    private int constructorTimes = 1;

    @Before
    public void setUp() {
        whensForConstructor(mockSecretsEmailConfigurationProvider);
        emailerDetectedAction = new EmailerDetectedAction(mockMimeMessageFactory, mockLogger, mockSender,
                mockSecretsEmailConfigurationProvider, mockFromAddressExceptionLogger, mockToAddressExceptionLogger);
        mimeMessageFactory = new MimeMessageFactory();
    }

    static void whensForConstructor(SecretsEmailConfigurationProvider mockSecretsEmailConfigurationProvider) {
        when(mockSecretsEmailConfigurationProvider.from()).thenReturn(FROM);
        when(mockSecretsEmailConfigurationProvider.tos()).thenReturn(TOS);
        when(mockSecretsEmailConfigurationProvider.subject()).thenReturn(SUBJECT);
        when(mockSecretsEmailConfigurationProvider.host()).thenReturn(HOST);
    }

    @After
    public void tearDown() {
        verifiesForConstructor(mockSecretsEmailConfigurationProvider, constructorTimes);

        verifyNoMoreInteractions(mockMimeMessageFactory);
        verifyNoMoreInteractions(mockLogger);
        verifyNoMoreInteractions(mockSender);
        verifyNoMoreInteractions(mockSecretsEmailConfigurationProvider);
        verifyNoMoreInteractions(mockMimeMessage);
        verifyNoMoreInteractions(mockFromAddressExceptionLogger);
        verifyNoMoreInteractions(mockToAddressExceptionLogger);
    }

    static void verifiesForConstructor(SecretsEmailConfigurationProvider mockSecretsEmailConfigurationProvider, int constructorTimes) {
        verify(mockSecretsEmailConfigurationProvider, times(constructorTimes)).from();
        verify(mockSecretsEmailConfigurationProvider, times(constructorTimes)).tos();
        verify(mockSecretsEmailConfigurationProvider, times(constructorTimes)).subject();
        verify(mockSecretsEmailConfigurationProvider, times(constructorTimes)).host();
    }

    @Test
    public void testConstructor() {
        final Properties properties = System.getProperties();
        final Object host = properties.get(HOST_KEY);
        assertEquals(HOST, host);
    }

    @Test
    public void testCreateFromAddressException() {
        final String illegalFrom = TOS.toString();
        when(mockSecretsEmailConfigurationProvider.from()).thenReturn(illegalFrom);
        constructorTimes = 2;
        new EmailerDetectedAction(mockMimeMessageFactory, mockLogger, mockSender, mockSecretsEmailConfigurationProvider,
                mockFromAddressExceptionLogger, mockToAddressExceptionLogger);
        final ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(mockFromAddressExceptionLogger).logError(eq(illegalFrom), argumentCaptor.capture());
        assertEquals(ILLEGAL_ADDRESS, argumentCaptor.getValue().getMessage());
    }

    @Test
    public void testCreateToAddressesException() {
        final List<String> illegalTos = ImmutableList.of(TOS.toString());
        when(mockSecretsEmailConfigurationProvider.tos()).thenReturn(illegalTos);
        constructorTimes = 2;
        new EmailerDetectedAction(mockMimeMessageFactory, mockLogger, mockSender, mockSecretsEmailConfigurationProvider,
                mockFromAddressExceptionLogger, mockToAddressExceptionLogger);
        final ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(mockToAddressExceptionLogger).logError(eq(illegalTos), argumentCaptor.capture());
        assertEquals(ILLEGAL_ADDRESS, argumentCaptor.getValue().getMessage());
    }

    @Test
    public void testConstructorNoEmailAddresses() {
        constructorTimes = 2;
        when(mockSecretsEmailConfigurationProvider.from()).thenReturn("");
        when(mockSecretsEmailConfigurationProvider.tos()).thenReturn(Collections.emptyList());
        when(mockSecretsEmailConfigurationProvider.subject()).thenReturn(SUBJECT);
        when(mockSecretsEmailConfigurationProvider.host()).thenReturn(HOST);

        new EmailerDetectedAction(mockMimeMessageFactory, mockLogger, mockSender, mockSecretsEmailConfigurationProvider,
                mockFromAddressExceptionLogger, mockToAddressExceptionLogger);
    }

    @Test
    public void testSendHappyCase() throws MessagingException {
        testSend(mockMimeMessage);
    }

    @Test
    public void testSendExceptionCase() throws MessagingException {
        MessagingException exceptionToThrow = new MessagingException("Test");
        doThrow(exceptionToThrow).when(mockSender).send(any(MimeMessage.class), eq(TO_ADDRESSES));

        testSend(mockMimeMessage);
        verify(mockLogger).error(SENDING_EXCEPTION_MSG, exceptionToThrow);
    }

    private void testSend(MimeMessage mimeMessage) throws MessagingException {
        when(mockMimeMessageFactory.createMimeMessage()).thenReturn(mimeMessage);

        emailerDetectedAction.send(FULLY_POPULATED_SPAN, SECRETS);

        verify(mockMimeMessageFactory).createMimeMessage();
        verify(mockMimeMessage).setFrom(new InternetAddress(FROM));
        verify(mockMimeMessage).setSubject(SUBJECT);
        verify(mockMimeMessage).setText(EMAIL_TEXT);
        verify(mockSender).send(mockMimeMessage, TO_ADDRESSES);
    }

    @Test(expected = MessagingException.class)
    public void testSenderImplSend() throws MessagingException {
        final SenderImpl sender = new SenderImpl();
        sender.send(mimeMessageFactory.createMimeMessage(), TO_ADDRESSES);
    }

    @Test
    public void testToAddressEmpty() {
        constructorTimes = 2;
        when(mockSecretsEmailConfigurationProvider.tos()).thenReturn(ImmutableList.of(""));
        emailerDetectedAction = new EmailerDetectedAction(mockMimeMessageFactory, mockLogger, mockSender,
                mockSecretsEmailConfigurationProvider, mockFromAddressExceptionLogger, mockToAddressExceptionLogger);
    }
}
