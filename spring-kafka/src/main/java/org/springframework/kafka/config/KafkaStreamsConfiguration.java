/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.util.Assert;

/**
 * Wrapper for {@link org.apache.kafka.streams.StreamsBuilder} properties. The framework
 * looks for a bean of this type with name 'defaultKafkaStreamsConfig' and auto-declares a
 * {@link StreamsBuilderFactoryBean} using it. The {@link Properties} class is too general
 * for such activity.
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
			Properties props = new Properties();
			props.putAll(this.configs);
			this.properties = props;
		}
		return this.properties;
	}

}
