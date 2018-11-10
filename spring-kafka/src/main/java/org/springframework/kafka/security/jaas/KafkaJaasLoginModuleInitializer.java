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
 * @since 1.3
 */
public class KafkaJaasLoginModuleInitializer implements SmartInitializingSingleton, DisposableBean {

	/**
	 * The key for the kafka client configuration entry.
	 */
	public static final String KAFKA_CLIENT_CONTEXT_NAME = "KafkaClient";

	/**
	 * Control flag values for login configuration.
	 */
	public enum ControlFlag {

		/**
		 * Required - The {@code LoginModule} is required to succeed. If it succeeds or
		 * fails, authentication still continues to proceed down the {@code LoginModule}
		 * list.
		 *
		 */
		REQUIRED,

		/**
		 * Requisite - The {@code LoginModule} is required to succeed. If it succeeds,
		 * authentication continues down the {@code LoginModule} list. If it fails,
		 * control immediately returns to the application (authentication does not proceed
		 * down the {@code LoginModule} list).
		 */
		REQUISITE,

		/**
		 * Sufficient - The {@code LoginModule} is not required to succeed. If it does
		 * succeed, control immediately returns to the application (authentication does
		 * not proceed down the {@code LoginModule} list). If it fails, authentication
		 * continues down the {@code LoginModule} list.
		 */
		SUFFICIENT,

		/**
		 * Optional - The {@code LoginModule} is not required to succeed. If it succeeds
		 * or fails, authentication still continues to proceed down the
		 * {@code LoginModule} list.
		 */
		OPTIONAL

	}

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

	public void setLoginModule(String loginModule) {
		Assert.notNull(loginModule, "cannot be null");
		this.loginModule = loginModule;
	}

	public void setControlFlag(ControlFlag controlFlag) {
		Assert.notNull(controlFlag, "cannot be null");
		switch (controlFlag) {
		case OPTIONAL:
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL;
			break;
		case REQUIRED:
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
			break;
		case REQUISITE:
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUISITE;
			break;
		case SUFFICIENT:
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT;
			break;
		default:
			throw new IllegalArgumentException(controlFlag + " is not a supported control flag");
		}
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
			configurationEntries.put(KAFKA_CLIENT_CONTEXT_NAME,
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
	public void destroy() {
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
