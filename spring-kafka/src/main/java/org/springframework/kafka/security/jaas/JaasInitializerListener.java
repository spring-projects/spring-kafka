/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.security.jaas;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.kafka.common.security.JaasUtils;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.Assert;

/**
 * Configures JAAS.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 *
 * @since 2.0
 */
public class JaasInitializerListener
		implements ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware, DisposableBean {

	private ApplicationContext applicationContext;

	private final boolean ignoreJavaLoginConfigParamSystemProperty;

	private final File placeholderJaasConfiguration;

	private final JaasLoginModuleConfiguration jaasLoginModuleConfiguration;

	public JaasInitializerListener(JaasLoginModuleConfiguration jaasLoginModuleConfiguration) throws IOException {
		// we ignore the system property if it wasn't originally set at launch
		this.ignoreJavaLoginConfigParamSystemProperty = (System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM) == null);
		this.placeholderJaasConfiguration = File.createTempFile("kafka-client-jaas-config-placeholder", "conf");
		this.placeholderJaasConfiguration.deleteOnExit();
		this.jaasLoginModuleConfiguration = jaasLoginModuleConfiguration;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void destroy() throws Exception {
		if (this.ignoreJavaLoginConfigParamSystemProperty) {
			System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
		}
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (event.getSource() == this.applicationContext) {
			// only use programmatic support if a file is not set via system property
			if (this.ignoreJavaLoginConfigParamSystemProperty && this.jaasLoginModuleConfiguration != null) {
				Map<String, AppConfigurationEntry[]> configurationEntries = new HashMap<>();
				AppConfigurationEntry kafkaClientConfigurationEntry = new AppConfigurationEntry(
						this.jaasLoginModuleConfiguration.getLoginModule(),
						this.jaasLoginModuleConfiguration.getControlFlagValue(),
						this.jaasLoginModuleConfiguration.getOptions());
				configurationEntries.put(JaasUtils.LOGIN_CONTEXT_CLIENT,
						new AppConfigurationEntry[] { kafkaClientConfigurationEntry });
				Configuration.setConfiguration(new InternalConfiguration(configurationEntries));
				// Workaround for a 0.9 client issue where even if the Configuration is
				// set
				// a system property check is performed.
				// Since the Configuration already exists, this will be ignored.
				if (this.placeholderJaasConfiguration != null) {
					System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM,
							this.placeholderJaasConfiguration.getAbsolutePath());
				}
			}
		}
	}

	private static class InternalConfiguration extends Configuration {

		private final Map<String, AppConfigurationEntry[]> configurationEntries;

		InternalConfiguration(Map<String, AppConfigurationEntry[]> configurationEntries) {
			Assert.notNull(configurationEntries, " cannot be null");
			Assert.notEmpty(configurationEntries, " cannot be empty");
			this.configurationEntries = configurationEntries;
		}

		@Override
		public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
			return this.configurationEntries.get(name);
		}
	}

}
