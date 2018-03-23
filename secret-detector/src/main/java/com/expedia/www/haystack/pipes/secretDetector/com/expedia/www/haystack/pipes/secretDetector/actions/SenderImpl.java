package com.expedia.www.haystack.pipes.secretDetector.com.expedia.www.haystack.pipes.secretDetector.actions;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;

public class SenderImpl implements EmailerDetectedAction.Sender {
    @Override
    public void send(Message message, Address[] toAddresses) throws MessagingException {
        Transport.send(message, toAddresses);
    }
}
