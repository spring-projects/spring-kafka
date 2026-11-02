/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.annotation;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

/**
 * {@code @Configuration} class that registers a {@link StreamsBuilderFactoryBean}
 * if {@link org.apache.kafka.streams.StreamsConfig} with the name
 * {@link KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_CONFIG_BEAN_NAME} is present
 * in the application context. Otherwise, a {@link UnsatisfiedDependencyException} is thrown.
 *
 * <p>This configuration class is automatically imported when using the @{@link EnableKafkaStreams}
 * annotation. See {@link EnableKafkaStreams} Javadoc for complete usage.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.1.4
 */
@Configuration(proxyBeanMethods = false)
public class KafkaStreamsDefaultConfiguration {

	/**
	 * The bean name for the {@link org.apache.kafka.streams.StreamsConfig} to be used for the default
	 * {@link StreamsBuilderFactoryBean} bean definition.
	 */
	public static final String DEFAULT_STREAMS_CONFIG_BEAN_NAME = "defaultKafkaStreamsConfig";

	/**
	 * The bean name for auto-configured default {@link StreamsBuilderFactoryBean}.
	 */
	public static final String DEFAULT_STREAMS_BUILDER_BEAN_NAME = "defaultKafkaStreamsBuilder";

	/**
	 * Bean for the default {@link StreamsBuilderFactoryBean}.
	 * @param streamsConfigProvider the streams config.
	 * @param configurerProvider the configurer.
	 *
	 * @return the factory bean.
	 */
	@Bean(name = DEFAULT_STREAMS_BUILDER_BEAN_NAME)
	public StreamsBuilderFactoryBean defaultKafkaStreamsBuilder(
			@Qualifier(DEFAULT_STREAMS_CONFIG_BEAN_NAME)
					ObjectProvider<KafkaStreamsConfiguration> streamsConfigProvider,
			ObjectProvider<StreamsBuilderFactoryBeanConfigurer> configurerProvider) {

		KafkaStreamsConfiguration streamsConfig = streamsConfigProvider.getIfAvailable();
		if (streamsConfig != null) {
			StreamsBuilderFactoryBean fb = new StreamsBuilderFactoryBean(streamsConfig);
			configurerProvider.orderedStream().forEach(configurer -> configurer.configure(fb));
			return fb;
		}
		else {
			throw new UnsatisfiedDependencyException(KafkaStreamsDefaultConfiguration.class.getName(),
					DEFAULT_STREAMS_BUILDER_BEAN_NAME, "streamsConfig", "There is no '" +
					DEFAULT_STREAMS_CONFIG_BEAN_NAME + "' " + KafkaStreamsConfiguration.class.getName() +
					" bean in the application context.\n" +
					"Consider declaring one or don't use @EnableKafkaStreams.");
		}
	}

}
