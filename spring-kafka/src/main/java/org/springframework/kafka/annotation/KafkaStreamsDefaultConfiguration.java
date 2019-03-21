/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.kafka.annotation;

import org.apache.kafka.streams.StreamsConfig;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KStreamBuilderFactoryBean;

/**
 * {@code @Configuration} class that registers a {@link KStreamBuilderFactoryBean}
 * if {@link StreamsConfig} with the name
 * {@link KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_CONFIG_BEAN_NAME} is present
 * in the application context. Otherwise a {@link UnsatisfiedDependencyException} is thrown.
 *
 * <p>This configuration class is automatically imported when using the @{@link EnableKafkaStreams}
 * annotation. See {@link EnableKafkaStreams} Javadoc for complete usage.
 *
 * @author Artem Bilan
 *
 * @since 1.1.4
 */
@Configuration
public class KafkaStreamsDefaultConfiguration {

	/**
	 * The bean name for the {@link StreamsConfig} to be used for the default
	 * {@link KStreamBuilderFactoryBean} bean definition.
	 */
	public static final String DEFAULT_STREAMS_CONFIG_BEAN_NAME = "defaultKafkaStreamsConfig";

	/**
	 * The bean name for auto-configured default {@link KStreamBuilderFactoryBean}.
	 */
	public static final String DEFAULT_KSTREAM_BUILDER_BEAN_NAME = "defaultKStreamBuilder";

	@Bean(name = DEFAULT_KSTREAM_BUILDER_BEAN_NAME)
	public KStreamBuilderFactoryBean defaultKStreamBuilder(
			@Qualifier(DEFAULT_STREAMS_CONFIG_BEAN_NAME) ObjectProvider<StreamsConfig> streamsConfigProvider) {
		StreamsConfig streamsConfig = streamsConfigProvider.getIfAvailable();
		if (streamsConfig != null) {
			return new KStreamBuilderFactoryBean(streamsConfig);
		}
		else {
			throw new UnsatisfiedDependencyException(KafkaStreamsDefaultConfiguration.class.getName(),
					DEFAULT_KSTREAM_BUILDER_BEAN_NAME, "streamsConfig", "There is no '" +
					DEFAULT_STREAMS_CONFIG_BEAN_NAME + "' StreamsConfig bean in the application context.\n" +
					"Consider to declare one or don't use @EnableKafkaStreams.");
		}
	}

}
