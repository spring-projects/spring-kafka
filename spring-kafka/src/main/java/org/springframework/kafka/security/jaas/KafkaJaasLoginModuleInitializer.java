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

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.util.Assert;

/**
 * Contains properties for setting up an {@link AppConfigurationEntry} that can be used
 * for the Kafka client.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 *
 * @since 2.0
 */
public class KafkaJaasLoginModuleInitializer implements SmartInitializingSingleton, DisposableBean {

	private final boolean ignoreJavaLoginConfigParamSystemProperty;

	private final File placeholderJaasConfiguration;

	private final Map<String, String> options = new HashMap<>();

	private String loginModule = "com.sun.security.auth.module.Krb5LoginModule";

	private AppConfigurationEntry.LoginModuleControlFlag controlFlag =
			AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;

	public KafkaJaasLoginModuleInitializer() throws IOException {
		// we ignore the system property if it wasn't originally set at launch
		this.ignoreJavaLoginConfigParamSystemProperty = (System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM) == null);
		this.placeholderJaasConfiguration = File.createTempFile("kafka-client-jaas-config-placeholder", "conf");
		this.placeholderJaasConfiguration.deleteOnExit();
	}

	public String getLoginModule() {
		return this.loginModule;
	}

	public void setLoginModule(String loginModule) {
		Assert.notNull(loginModule, "cannot be null");
		this.loginModule = loginModule;
	}

	public String getControlFlag() {
		return this.controlFlag.toString();
	}

	public void setControlFlag(String controlFlag) {
		Assert.notNull(controlFlag, "cannot be null");
		switch (controlFlag.toUpperCase()) {
		case "OPTIONAL":
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL;
			break;
		case "REQUIRED":
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
			break;
		case "REQUISITE":
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUISITE;
			break;
		case "SUFFICIENT":
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT;
			break;
		default:
			throw new IllegalArgumentException(controlFlag + " is not a supported control flag");
		}
	}

	public AppConfigurationEntry.LoginModuleControlFlag getControlFlagValue() {
		return this.controlFlag;
	}

	public Map<String, String> getOptions() {
		return this.options;
	}

	public void setOptions(Map<String, String> options) {
		this.options.clear();
		this.options.putAll(options);
	}

	@Override
	public void afterSingletonsInstantiated() {
		// only use programmatic support if a file is not set via system property
		if (this.ignoreJavaLoginConfigParamSystemProperty) {
			Map<String, AppConfigurationEntry[]> configurationEntries = new HashMap<>();
			AppConfigurationEntry kafkaClientConfigurationEntry = new AppConfigurationEntry(
					this.loginModule,
					this.controlFlag,
					this.options);
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

	@Override
	public void destroy() throws Exception {
		if (this.ignoreJavaLoginConfigParamSystemProperty) {
			System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
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
