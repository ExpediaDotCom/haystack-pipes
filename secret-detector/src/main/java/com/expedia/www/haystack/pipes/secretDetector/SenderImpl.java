package com.expedia.www.haystack.pipes.secretDetector;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;

class SenderImpl implements Emailer.Sender {
    @Override
    public void send(Message message, Address[] toAddresses) throws MessagingException {
        Transport.send(message, toAddresses);
    }
}
