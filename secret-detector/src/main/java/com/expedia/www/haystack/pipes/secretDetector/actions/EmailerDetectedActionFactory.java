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
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EmailerDetectedActionFactory implements DetectedActionFactory {

    private final EmailerDetectedAction.Factory emailerFactory;
    private final Logger emailerLogger;
    private final EmailerDetectedAction.Sender sender;
    private final SecretsEmailConfigurationProvider secretsEmailConfigurationProvider;

    @Autowired
    public EmailerDetectedActionFactory(EmailerDetectedAction.Factory emailerFactory,
                                        Logger emailerLogger,
                                        EmailerDetectedAction.Sender sender,
                                        SecretsEmailConfigurationProvider secretsEmailConfigurationProvider) {
        this.emailerFactory = emailerFactory;
        this.emailerLogger = emailerLogger;
        this.sender = sender;
        this.secretsEmailConfigurationProvider = secretsEmailConfigurationProvider;
    }

    @Override
    public DetectedAction create() {
        return new EmailerDetectedAction(emailerFactory, emailerLogger, sender, secretsEmailConfigurationProvider);
    }
}
