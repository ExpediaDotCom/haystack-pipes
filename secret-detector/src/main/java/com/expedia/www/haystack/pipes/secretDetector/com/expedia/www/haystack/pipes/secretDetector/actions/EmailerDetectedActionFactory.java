package com.expedia.www.haystack.pipes.secretDetector.com.expedia.www.haystack.pipes.secretDetector.actions;

import com.expedia.www.haystack.pipes.secretDetector.SecretsConfigurationProvider;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EmailerDetectedActionFactory implements DetectedActionFactory {

    private final EmailerDetectedAction.Factory emailerFactory;
    private final Logger emailerLogger;
    private final EmailerDetectedAction.Sender sender;
    private final SecretsConfigurationProvider secretsConfigurationProvider;

    @Autowired
    public EmailerDetectedActionFactory(EmailerDetectedAction.Factory emailerFactory,
                                        Logger emailerLogger,
                                        EmailerDetectedAction.Sender sender,
                                        SecretsConfigurationProvider secretsConfigurationProvider) {
        this.emailerFactory = emailerFactory;
        this.emailerLogger = emailerLogger;
        this.sender = sender;
        this.secretsConfigurationProvider = secretsConfigurationProvider;
    }

    @Override
    public DetectedAction create() {
        return new EmailerDetectedAction(emailerFactory, emailerLogger, sender, secretsConfigurationProvider);
    }
}
