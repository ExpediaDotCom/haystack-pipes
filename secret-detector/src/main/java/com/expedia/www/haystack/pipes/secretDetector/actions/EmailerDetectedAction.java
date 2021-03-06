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

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.secretDetector.config.SecretsEmailConfigurationProvider;
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.List;
import java.util.Map;

@Component
public class EmailerDetectedAction implements DetectedAction {
    @VisibleForTesting
    static final String TEXT_TEMPLATE =
            "Confidential data has been found in a span: service [%s] operation [%s] span [%s] trace [%s] tag(s) [%s]";
    @VisibleForTesting
    static final String SENDING_EXCEPTION_MSG = "Problem sending eMail";
    @VisibleForTesting
    static final String HOST_KEY = "mail.smtp.host";

    private final MimeMessageFactory mimeMessageFactory;
    private final Sender sender;
    private final Logger logger;
    private final FromAddressExceptionLogger fromAddressExceptionLogger;
    private final ToAddressExceptionLogger toAddressExceptionLogger;

    private final Address from;
    private final Address[] toAddresses;
    private final String subject;

    @Autowired
    public EmailerDetectedAction(MimeMessageFactory mimeMessageFactory,
                          Logger emailerDetectedActionLogger,
                          Sender sender,
                          SecretsEmailConfigurationProvider secretsEmailConfigurationProvider,
                          FromAddressExceptionLogger fromAddressExceptionLogger,
                          ToAddressExceptionLogger toAddressExceptionLogger) {
        this.mimeMessageFactory = mimeMessageFactory;
        this.logger = emailerDetectedActionLogger;
        this.sender = sender;
        this.fromAddressExceptionLogger = fromAddressExceptionLogger;
        this.toAddressExceptionLogger = toAddressExceptionLogger;

        this.from = createFromAddress(secretsEmailConfigurationProvider);
        this.toAddresses = createToAddresses(secretsEmailConfigurationProvider);
        this.subject = secretsEmailConfigurationProvider.subject();
        System.getProperties().setProperty(HOST_KEY, secretsEmailConfigurationProvider.host());
    }

    private Address createFromAddress(SecretsEmailConfigurationProvider secretsEmailConfigurationProvider) {
        final String sFrom = secretsEmailConfigurationProvider.from();
        if(!StringUtils.isBlank(sFrom)) {
            try {
                return new InternetAddress(sFrom);
            } catch (AddressException e) {
                fromAddressExceptionLogger.logError(sFrom, e);
            }
        }
        return null;
    }

    private Address[] createToAddresses(SecretsEmailConfigurationProvider secretsEmailConfigurationProvider) {
        final List<String> tos = secretsEmailConfigurationProvider.tos();
        if(tos.size() > 0) {
            try {
                final Address[] addresses = new Address[tos.size()];
                for (int i = 0; i < tos.size(); i++) {
                    final String address = tos.get(i);
                    if(!StringUtils.isBlank(address)) {
                        addresses[i] = new InternetAddress(address);
                    }
                }
                return addresses;
            } catch (AddressException e) {
                toAddressExceptionLogger.logError(tos, e);
            }
        }
        return null;
    }

    @Override
    public void send(Span span, Map<String, List<String>> mapOfTypeToKeysOfSecrets) {
        final MimeMessage message = mimeMessageFactory.createMimeMessage();
        try {
            message.setFrom(from);
            message.setSubject(subject);
            final String text = getEmailText(span, mapOfTypeToKeysOfSecrets);
            message.setText(text);
            sender.send(message, toAddresses);
        } catch (MessagingException e) {
            logger.error(SENDING_EXCEPTION_MSG, e);
        }
    }

    private static String getEmailText(Span span, Map<String, List<String>> mapOfTypeToKeysOfSecrets) {
        return String.format(TEXT_TEMPLATE, span.getServiceName(), span.getOperationName(), span.getSpanId(),
                span.getTraceId(), mapOfTypeToKeysOfSecrets.toString());
    }

    public interface Sender {
        void send(Message message, Address[] toAddresses) throws MessagingException;
    }

    public static class MimeMessageFactory {
        MimeMessage createMimeMessage() {
            return new MimeMessage(Session.getDefaultInstance(System.getProperties()));
        }
    }

}
