/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.jspecify.annotations.Nullable;

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

	private @Nullable Properties properties;

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
		return (Properties) this.properties.clone();
	}

}
