/*
 * Copyright 2017 Expedia, Inc.
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
package com.expedia.www.haystack.pipes;

import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.context.environment.Environment;
import org.cfg4j.source.system.EnvironmentVariablesConfigurationSource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * {@link ConfigurationSource} providing all environment variables under the specified {@link Environment}
 * namespaced prefix
 */
public class ChangeEnvVarsToLowerCaseConfigurationSource extends EnvironmentVariablesConfigurationSource {

    private final String prefixOfStringsToConvertToLowerCase;

    /**
     * Constructs a new ChangeEnvVarsToLowerCaseConfigurationSource object that will convert environment variable
     * keys (not their values) to lower case if they start with the specified prefix. Since environment variables are
     * by convention upper case, the prefix will probably be upper case as well, but this is not required.
     *
     * @param prefixOfStringsToConvertToLowerCase indicates what environment variables should be changed to lower case.
     */
    ChangeEnvVarsToLowerCaseConfigurationSource(String prefixOfStringsToConvertToLowerCase) {
        this.prefixOfStringsToConvertToLowerCase = prefixOfStringsToConvertToLowerCase;
    }

    @Override
    public Properties getConfiguration(Environment environment) {
        final Properties properties = super.getConfiguration(environment);
        final Iterator<Map.Entry<Object, Object>> iterator = properties.entrySet().iterator();
        final Map<String, Object> toAdd = new HashMap<>();
        while (iterator.hasNext()) {
            final Map.Entry<Object, Object> next = iterator.next();
            final String key = (String) next.getKey();
            if (key.startsWith(prefixOfStringsToConvertToLowerCase)) {
                toAdd.put(key.toLowerCase(), next.getValue());
                iterator.remove();
            }
        }
        properties.putAll(toAdd);

        return properties;
    }

    @Override
    public String toString() {
        return "ChangeEnvVarsToLowerCaseConfigurationSource{}";
    }
}
