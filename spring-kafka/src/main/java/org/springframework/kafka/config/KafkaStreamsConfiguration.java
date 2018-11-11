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

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.util.Assert;

/**
 * Wrapper for {@link org.apache.kafka.streams.StreamsBuilder} properties.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public class KafkaStreamsConfiguration {

	private final Map<String, Object> configs;

	private final DefaultConversionService conversionService = new DefaultConversionService();

	private Properties properties;

	public KafkaStreamsConfiguration(Map<String, Object> configs) {
		Assert.notNull(configs, "Configuration map cannot be null");
		this.configs = new HashMap<>(configs);
		// Not lambdas so we retain type information
		this.conversionService.addConverter(new Converter<Class<?>, String>() {

			@Override
			public String convert(Class<?> c) {
				return c.getName();
			}

		});
		this.conversionService.addConverter(new Converter<List<?>, String>() {

			@Override
			public String convert(List<?> l) {
				String value = l.toString();
				// trim [...] - revert to comma-delimited list
				return value.substring(1, value.length() - 1);
			}

		});
	}

	/**
	 * Return the configuration map as a {@link Properties}.
	 * @return the properties.
	 */
	public Properties asProperties() {
		if (this.properties == null) {
			Properties props = new Properties();
			this.configs.forEach((k, v) -> {
				String value;
				if (this.conversionService.canConvert(v.getClass(), String.class)) {
					value = this.conversionService.convert(v, String.class);
				}
				else {
					value = v.toString();
				}
				props.setProperty(k, value);
			});
			this.properties = props;
		}
		return this.properties;
	}

}
