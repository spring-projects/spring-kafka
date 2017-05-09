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

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

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
public class JaasLoginModuleConfiguration {

	private final Map<String, String> options = new HashMap<>();

	private String loginModule = "com.sun.security.auth.module.Krb5LoginModule";

	private AppConfigurationEntry.LoginModuleControlFlag controlFlag =
			AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;

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
		if (AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL.equals(controlFlag)) {
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL;
		}
		else if (AppConfigurationEntry.LoginModuleControlFlag.REQUIRED.equals(controlFlag)) {
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
		}
		else if (AppConfigurationEntry.LoginModuleControlFlag.REQUISITE.equals(controlFlag)) {
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUISITE;
		}
		else if (AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT.equals(controlFlag)) {
			this.controlFlag = AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT;
		}
		else {
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

}
