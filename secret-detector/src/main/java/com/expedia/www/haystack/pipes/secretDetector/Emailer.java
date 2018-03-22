package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Span;
import com.netflix.servo.util.VisibleForTesting;
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

@Component
public class Emailer {
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

    final private Factory factory;
    final private Sender sender;
    final private Logger logger;

    final private Address from;
    final private Address[] toAddresses;
    final private String subject;

    @Autowired
    Emailer(Factory emailerFactory,
            Logger emailerLogger,
            Sender sender,
            SecretsConfigurationProvider secretsConfigurationProvider) {
        this.factory = emailerFactory;
        this.logger = emailerLogger;
        this.sender = sender;

        this.from = createFromAddress(secretsConfigurationProvider);
        this.toAddresses = createToAddresses(secretsConfigurationProvider);
        this.subject = secretsConfigurationProvider.subject();
        System.getProperties().setProperty(HOST_KEY, secretsConfigurationProvider.host());
    }

    private Address createFromAddress(SecretsConfigurationProvider secretsConfigurationProvider) {
        final String sFrom = secretsConfigurationProvider.from();
        try {
            return new InternetAddress(sFrom);
        } catch (AddressException e) {
            final String message = String.format(FROM_ADDRESS_EXCEPTION_MSG, sFrom);
            logger.error(message);
        }
        return null;
    }

    private Address[] createToAddresses(SecretsConfigurationProvider secretsConfigurationProvider) {
        final List<String> tos = secretsConfigurationProvider.tos();
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

    void send(Span span, List<String> listOfKeysOfSecrets) {
        final MimeMessage message = factory.createMimeMessage();
        try {
            message.setFrom(from);
            message.setSubject(subject);
            final String text = String.format(TEXT_TEMPLATE, span.getServiceName(), span.getOperationName(),
                    span.getSpanId(), span.getTraceId(), listOfKeysOfSecrets.toString());
            message.setText(text);
            sender.send(message, toAddresses);
        } catch (MessagingException e) {
            logger.error(SENDING_EXCEPTION_MSG);
        }
    }

    interface Sender {
        void send(Message message, Address[] toAddresses) throws MessagingException;
    }

    static class Factory {
        MimeMessage createMimeMessage() {
            return new MimeMessage(Session.getDefaultInstance(System.getProperties()));
        }
    }
}
