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
package com.expedia.www.haystack.pipes.secretDetector.config;

import com.expedia.www.haystack.pipes.secretDetector.SpringConfig;
import com.expedia.www.haystack.pipes.secretDetector.actions.DetectedAction;
import com.expedia.www.haystack.pipes.secretDetector.actions.DetectedActionFactory;
import com.netflix.servo.util.VisibleForTesting;
import org.cfg4j.provider.ConfigurationProvider;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class ActionsConfigurationProvider implements ActionsConfig {
    @VisibleForTesting
    static final String HAYSTACK_SECRETS_CONFIG_PREFIX = "haystack.secretsnotifications";
    @VisibleForTesting
    static final String PROBLEM_USING_CONFIGURATION = "Problem using %s configuration [%s]";

    private final Logger logger;
    private final ActionsConfig actionsConfig;

    @Autowired
    public ActionsConfigurationProvider(Logger actionsConfigurationProviderLogger,
                                        ConfigurationProvider configurationProvider) {
        this(actionsConfigurationProviderLogger,
                configurationProvider.bind(HAYSTACK_SECRETS_CONFIG_PREFIX, ActionsConfig.class));
    }

    ActionsConfigurationProvider(Logger actionsConfigurationProviderLogger, ActionsConfig actionsConfig) {
        this.logger = actionsConfigurationProviderLogger;
        this.actionsConfig = actionsConfig;
    }

    @Override
    public List<String> actionfactories() {
        return actionsConfig.actionfactories();
    }

    @Override
    public String mainbean() {
        return actionsConfig.mainbean();
    }

    public List<DetectedAction> getDetectedActions() {
        final List<String> sActionFactories = actionfactories();
        final List<DetectedAction> detectedActions = new ArrayList<>(sActionFactories.size());
        final AnnotationConfigApplicationContext annotationConfigApplicationContext =
                new AnnotationConfigApplicationContext(SpringConfig.class);
        for (final String sActionFactory : sActionFactories) {
            try {
                final DetectedActionFactory factoryBean =
                        (DetectedActionFactory) annotationConfigApplicationContext.getBean(sActionFactory);
                detectedActions.add(factoryBean.create());
            } catch (Exception e) {
                final String configName = HAYSTACK_SECRETS_CONFIG_PREFIX + ".actionfactories";
                logger.error(String.format(PROBLEM_USING_CONFIGURATION, configName, sActionFactory));
                throw new RuntimeException(e);
            }
        }
        return detectedActions;
    }
}
