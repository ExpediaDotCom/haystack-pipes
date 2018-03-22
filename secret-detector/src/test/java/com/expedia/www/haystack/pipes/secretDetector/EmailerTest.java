package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.www.haystack.pipes.secretDetector.Emailer.Factory;
import com.expedia.www.haystack.pipes.secretDetector.Emailer.Sender;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import javax.mail.Address;
import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.List;
import java.util.Properties;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.FULLY_POPULATED_SPAN;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.OPERATION_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.SERVICE_NAME;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.SPAN_ID;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.TRACE_ID;
import static com.expedia.www.haystack.pipes.secretDetector.Emailer.FROM_ADDRESS_EXCEPTION_MSG;
import static com.expedia.www.haystack.pipes.secretDetector.Emailer.HOST_KEY;
import static com.expedia.www.haystack.pipes.secretDetector.Emailer.SENDING_EXCEPTION_MSG;
import static com.expedia.www.haystack.pipes.secretDetector.Emailer.TEXT_TEMPLATE;
import static com.expedia.www.haystack.pipes.secretDetector.Emailer.TOS_ADDRESS_EXCEPTION_MSG;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EmailerTest {
    private final static String FROM = RANDOM.nextInt(Integer.MAX_VALUE) + "@expedia.com";
    private final static String TO1 = RANDOM.nextInt(Integer.MAX_VALUE) + "@expedia.com";
    private final static String TO2 = RANDOM.nextInt(Integer.MAX_VALUE) + "@expedia.com";
    private final static List<String> TOS = ImmutableList.of(TO1, TO2);
    private final static Address[] TO_ADDRESSES = new Address[TOS.size()];
    static {
        for(int i = 0; i < TOS.size() ; i++) {
            try {
                TO_ADDRESSES[i] = new InternetAddress(TOS.get(i));
            } catch (AddressException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private final static String SUBJECT = RANDOM.nextLong() + "SUBJECT";
    private final static String HOST = "localhost"; //RANDOM.nextLong() + "HOST";
    private static final String SECRET = RANDOM.nextLong() + "SECRET";
    private static final List<String> SECRETS = ImmutableList.of(SECRET);
    private static final String EMAIL_TEXT =
            String.format(TEXT_TEMPLATE, SERVICE_NAME, OPERATION_NAME, SPAN_ID, TRACE_ID, SECRETS);

    @Mock
    private Factory mockFactory;
    @Mock
    private Logger mockLogger;
    @Mock
    private Sender mockSender;
    @Mock
    private SecretsConfigurationProvider mockSecretsConfigurationProvider;
    @Mock
    private MimeMessage mockMimeMessage;

    private Emailer emailer;
    private Factory factory;
    private int constructorTimes = 1;

    @Before
    public void setUp() {
        when(mockSecretsConfigurationProvider.from()).thenReturn(FROM);
        when(mockSecretsConfigurationProvider.tos()).thenReturn(TOS);
        when(mockSecretsConfigurationProvider.subject()).thenReturn(SUBJECT);
        when(mockSecretsConfigurationProvider.host()).thenReturn(HOST);
        emailer = new Emailer(mockFactory, mockLogger, mockSender, mockSecretsConfigurationProvider);
        factory = new Factory();
    }

    @After
    public void tearDown() {
        verify(mockSecretsConfigurationProvider, times(constructorTimes)).from();
        verify(mockSecretsConfigurationProvider, times(constructorTimes)).tos();
        verify(mockSecretsConfigurationProvider, times(constructorTimes)).subject();
        verify(mockSecretsConfigurationProvider, times(constructorTimes)).host();

        verifyNoMoreInteractions(mockFactory, mockLogger, mockSender, mockSecretsConfigurationProvider);
        verifyNoMoreInteractions(mockMimeMessage);
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
        when(mockSecretsConfigurationProvider.from()).thenReturn(illegalFrom);
        testConstructorAddressException(String.format(FROM_ADDRESS_EXCEPTION_MSG, illegalFrom));
    }

    @Test
    public void testCreateToAddressesException() {
        final List<String> illegalTos = ImmutableList.of(TOS.toString());
        when(mockSecretsConfigurationProvider.tos()).thenReturn(illegalTos);
        testConstructorAddressException(String.format(TOS_ADDRESS_EXCEPTION_MSG, illegalTos));
    }

    private void testConstructorAddressException(String message) {
        constructorTimes = 2;
        new Emailer(mockFactory, mockLogger, mockSender, mockSecretsConfigurationProvider);
        verify(mockLogger).error(message);
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
        verify(mockLogger).error(SENDING_EXCEPTION_MSG);
    }

    private void testSend(MimeMessage mimeMessage) throws MessagingException {
        when(mockFactory.createMimeMessage()).thenReturn(mimeMessage);

        emailer.send(FULLY_POPULATED_SPAN, SECRETS);

        verify(mockFactory).createMimeMessage();
        verify(mockMimeMessage).setFrom(new InternetAddress(FROM));
        verify(mockMimeMessage).setSubject(SUBJECT);
        verify(mockMimeMessage).setText(EMAIL_TEXT);
        verify(mockSender).send(mockMimeMessage, TO_ADDRESSES);
    }

    @Test(expected = MessagingException.class)
    public void testSenderImplSend() throws MessagingException {
        final SenderImpl sender = new SenderImpl();
        sender.send(factory.createMimeMessage(), TO_ADDRESSES);
    }
}
