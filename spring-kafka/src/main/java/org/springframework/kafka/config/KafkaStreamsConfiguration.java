/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.kafka.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.StreamsBuilder;

import org.springframework.util.Assert;

/**
 * Wrapper for {@link StreamsBuilder} properties.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public class KafkaStreamsConfiguration {

	private final Map<String, Object> configs;

	private Properties properties;

	public KafkaStreamsConfiguration(Map<String, Object> configs) {
		Assert.notNull(configs, "Configuration map cannot be null");
		this.configs = new HashMap<>(configs);
	}

	/**
	 * Return the configuration map as a {@link Properties}.
	 * @return the properties.
	 */
	public Properties asProperties() {
		if (this.properties == null) {
			Properties properties = new Properties();
			this.configs.forEach((k, v) -> {
				String value = v.toString();
				if (v instanceof List && value.length() > 1) {
					// trim [...] - revert to comma-delimited list
					value = value.substring(1, value.length() - 1);
				}
				properties.setProperty(k, value);
			});
			this.properties = properties;
		}
		return this.properties;
	}

}
