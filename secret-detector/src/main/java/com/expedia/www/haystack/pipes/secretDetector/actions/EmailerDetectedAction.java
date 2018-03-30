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
import org.slf4j.Logger;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.List;

public class EmailerDetectedAction implements DetectedAction {
    @VisibleForTesting
    static final String TEXT_TEMPLATE =
            "Confidential data has been found in a span: service [%s] operation [%s] span [%s] trace [%s] tag(s) [%s]";
    @VisibleForTesting
    static final String FROM_ADDRESS_EXCEPTION_MSG = "Problem using eMail configurations: from [%s]";
    @VisibleForTesting
    static final String TOS_ADDRESS_EXCEPTION_MSG = "Problem using eMail configurations: tos [%s]";
    @VisibleForTesting
    static final String SENDING_EXCEPTION_MSG = "Problem sending eMail";
    @VisibleForTesting
    static final String HOST_KEY = "mail.smtp.host";

    private final Factory factory;
    private final Sender sender;
    private final Logger logger;

    private final Address from;
    private final Address[] toAddresses;
    private final String subject;

    EmailerDetectedAction(Factory emailerFactory,
                          Logger emailerLogger,
                          Sender sender,
                          SecretsEmailConfigurationProvider secretsEmailConfigurationProvider) {
        this.factory = emailerFactory;
        this.logger = emailerLogger;
        this.sender = sender;

        this.from = createFromAddress(secretsEmailConfigurationProvider);
        this.toAddresses = createToAddresses(secretsEmailConfigurationProvider);
        this.subject = secretsEmailConfigurationProvider.subject();
        System.getProperties().setProperty(HOST_KEY, secretsEmailConfigurationProvider.host());
    }

    private Address createFromAddress(SecretsEmailConfigurationProvider secretsEmailConfigurationProvider) {
        final String sFrom = secretsEmailConfigurationProvider.from();
        try {
            return new InternetAddress(sFrom);
        } catch (AddressException e) {
            final String message = String.format(FROM_ADDRESS_EXCEPTION_MSG, sFrom);
            logger.error(message);
        }
        return null;
    }

    private Address[] createToAddresses(SecretsEmailConfigurationProvider secretsEmailConfigurationProvider) {
        final List<String> tos = secretsEmailConfigurationProvider.tos();
        try {
            final Address[] addresses = new Address[tos.size()];
            for(int i = 0; i < tos.size() ; i++) {
                addresses[i] = new InternetAddress(tos.get(i));
            }
            return addresses;
        } catch(AddressException e) {
            final String message = String.format(TOS_ADDRESS_EXCEPTION_MSG, tos);
            logger.error(message);
        }
        return null;
    }

    @Override
    public void send(Span span, List<String> listOfKeysOfSecrets) {
        final MimeMessage message = factory.createMimeMessage();
        try {
            message.setFrom(from);
            message.setSubject(subject);
            final String text = getEmailText(span, listOfKeysOfSecrets);
            message.setText(text);
            sender.send(message, toAddresses);
        } catch (MessagingException e) {
            logger.error(SENDING_EXCEPTION_MSG, e);
        }
    }

    public static String getEmailText(Span span, List<String> listOfKeysOfSecrets) {
        return String.format(TEXT_TEMPLATE, span.getServiceName(), span.getOperationName(), span.getSpanId(),
                span.getTraceId(), listOfKeysOfSecrets.toString());
    }

    public interface Sender {
        void send(Message message, Address[] toAddresses) throws MessagingException;
    }

    public static class Factory {
        MimeMessage createMimeMessage() {
            return new MimeMessage(Session.getDefaultInstance(System.getProperties()));
        }
    }

}
