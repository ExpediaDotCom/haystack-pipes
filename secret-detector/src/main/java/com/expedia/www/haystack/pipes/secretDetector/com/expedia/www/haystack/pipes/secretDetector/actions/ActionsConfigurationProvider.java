package com.expedia.www.haystack.pipes.secretDetector.com.expedia.www.haystack.pipes.secretDetector.actions;

import com.expedia.www.haystack.pipes.secretDetector.SpringConfig;
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
    ActionsConfigurationProvider(Logger actionsConfigurationProviderLogger,
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
