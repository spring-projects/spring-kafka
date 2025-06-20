/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.kafka.retrytopic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.util.StringUtils;

/**
 *
 * Resolves a {@link ConcurrentKafkaListenerContainerFactory} to be used by the
 * {@link RetryTopicConfiguration}.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 * @see ListenerContainerFactoryConfigurer
 *
 */
public class ListenerContainerFactoryResolver {

	private final BeanFactory beanFactory;

	private final List<FactoryResolver> mainEndpointResolvers;

	private final List<FactoryResolver> retryEndpointResolvers;

	private final Cache mainEndpointCache;

	private final Cache retryEndpointCache;

	public ListenerContainerFactoryResolver(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		this.mainEndpointCache = new Cache();
		this.retryEndpointCache = new Cache();

		this.mainEndpointResolvers = Arrays.asList(
				this.mainEndpointCache::fromCache,
				(fromKafkaListenerAnnotation, configuration) -> fromKafkaListenerAnnotation,
				(fromKLAnnotation, configuration) -> configuration.factoryFromRetryTopicConfiguration,
				(fromKLAnnotation, configuration) -> fromBeanName(configuration.listenerContainerFactoryName),
				(fromKLAnnotation, configuration) ->
						fromBeanName("internalRetryTopicListenerContainerFactory"),
				(fromKLAnnotation, configuration) ->
						fromBeanName(RetryTopicBeanNames.DEFAULT_LISTENER_CONTAINER_FACTORY_BEAN_NAME));

		this.retryEndpointResolvers = Arrays.asList(
				this.retryEndpointCache::fromCache,
				(fromKLAnnotation, configuration) -> configuration.factoryFromRetryTopicConfiguration,
				(fromKLAnnotation, configuration) -> fromBeanName(configuration.listenerContainerFactoryName),
				(fromKLAnnotation, configuration) -> fromKLAnnotation,
				(fromKLAnnotation, configuration) ->
						fromBeanName("internalRetryTopicListenerContainerFactory"),
				(fromKLAnnotation, configuration) ->
						fromBeanName(RetryTopicBeanNames.DEFAULT_LISTENER_CONTAINER_FACTORY_BEAN_NAME));
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactoryForMainEndpoint(
			@Nullable KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotationInstance,
			String defaultContainerFactoryBeanName, Configuration config) {

		KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation =
				getFactoryFromKLA(factoryFromKafkaListenerAnnotationInstance, defaultContainerFactoryBeanName);
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = resolveFactory(this.mainEndpointResolvers,
				factoryFromKafkaListenerAnnotation, config);
		return this.mainEndpointCache.addIfAbsent(factoryFromKafkaListenerAnnotation, config, resolvedFactory); // NOSONAR
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactoryForRetryEndpoint(
			@Nullable KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotationInstance,
			String defaultContainerFactoryBeanName, Configuration config) {

		KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation =
				getFactoryFromKLA(factoryFromKafkaListenerAnnotationInstance, defaultContainerFactoryBeanName);
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = resolveFactory(this.retryEndpointResolvers,
				factoryFromKafkaListenerAnnotation, config);
		return this.retryEndpointCache.addIfAbsent(factoryFromKafkaListenerAnnotation, config, resolvedFactory); // NOSONAR
	}

	@Nullable
	private KafkaListenerContainerFactory<?> getFactoryFromKLA(
			@Nullable KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotationInstance,
			String defaultContainerFactoryBeanName) {

		KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation =
				factoryFromKafkaListenerAnnotationInstance;
		if (factoryFromKafkaListenerAnnotation == null) {
			factoryFromKafkaListenerAnnotation = fromBeanName(defaultContainerFactoryBeanName);
		}
		return factoryFromKafkaListenerAnnotation;
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactory(List<FactoryResolver> factoryResolvers,
			@Nullable KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation,
			Configuration config) {

		ConcurrentKafkaListenerContainerFactory<?, ?> verifiedFactoryFromKafkaListenerAnnotation = verifyClass(
				factoryFromKafkaListenerAnnotation);
		return factoryResolvers
				.stream()
				.map(resolver -> Optional.ofNullable(
						resolver.resolveFactory(verifiedFactoryFromKafkaListenerAnnotation, config)))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Could not resolve a viable " +
						"ConcurrentKafkaListenerContainerFactory to configure the retry topic. " +
						"Try creating a bean with name " +
						RetryTopicBeanNames.DEFAULT_LISTENER_CONTAINER_FACTORY_BEAN_NAME));
	}

	@Nullable
	private ConcurrentKafkaListenerContainerFactory<?, ?> verifyClass(
			@Nullable KafkaListenerContainerFactory<?> fromKafkaListenerAnnotationFactory) {

		return fromKafkaListenerAnnotationFactory != null
				&& ConcurrentKafkaListenerContainerFactory.class.isAssignableFrom(fromKafkaListenerAnnotationFactory.getClass())
				? (ConcurrentKafkaListenerContainerFactory<?, ?>) fromKafkaListenerAnnotationFactory
				: null;
	}

	@Nullable
	private ConcurrentKafkaListenerContainerFactory<?, ?> fromBeanName(@Nullable String factoryBeanName) {
		try {
			return StringUtils.hasText(factoryBeanName)
					? this.beanFactory.getBean(factoryBeanName, ConcurrentKafkaListenerContainerFactory.class)
					: null;
		}
		catch (NoSuchBeanDefinitionException ex) {
			return null;
		}
	}

	private interface FactoryResolver {
		@Nullable
		ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactory(@Nullable ConcurrentKafkaListenerContainerFactory<?, ?> candidate,
																	Configuration configuration);
	}

	static class Configuration {

		private final @Nullable ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromRetryTopicConfiguration;

		private final @Nullable String listenerContainerFactoryName;

		Configuration(@Nullable ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromRetryTopicConfiguration,
					@Nullable String listenerContainerFactoryName) {
			this.factoryFromRetryTopicConfiguration = factoryFromRetryTopicConfiguration;
			this.listenerContainerFactoryName = listenerContainerFactoryName;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Configuration that = (Configuration) o;
			return Objects.equals(this.factoryFromRetryTopicConfiguration, that.factoryFromRetryTopicConfiguration)
					&& Objects.equals(this.listenerContainerFactoryName, that.listenerContainerFactoryName);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.factoryFromRetryTopicConfiguration, this.listenerContainerFactoryName);
		}

	}

	static class Cache {

		private final Map<Key, ConcurrentKafkaListenerContainerFactory<?, ?>> cacheMap;

		Cache() {
			this.cacheMap = new HashMap<>();
		}

		ConcurrentKafkaListenerContainerFactory<?, ?> addIfAbsent(@Nullable KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation,
																Configuration config,
																ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory) {
			synchronized (this.cacheMap) {
				Key key = cacheKey(factoryFromKafkaListenerAnnotation, config);
				this.cacheMap.putIfAbsent(key, resolvedFactory);
				return resolvedFactory;
			}
		}

		@Nullable
		ConcurrentKafkaListenerContainerFactory<?, ?> fromCache(@Nullable KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation,
																Configuration config) {
			synchronized (this.cacheMap) {
				return this.cacheMap.get(cacheKey(factoryFromKafkaListenerAnnotation, config));
			}
		}

		private Key cacheKey(@Nullable KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
			return new Key(factoryFromKafkaListenerAnnotation, config);
		}

		static class Key {

			private final @Nullable KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation;

			private final Configuration config;

			Key(@Nullable KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
				this.factoryFromKafkaListenerAnnotation = factoryFromKafkaListenerAnnotation;
				this.config = config;
			}

			@Override
			public boolean equals(Object o) {
				if (this == o) {
					return true;
				}
				if (o == null || getClass() != o.getClass()) {
					return false;
				}
				Key key = (Key) o;
				return Objects.equals(this.factoryFromKafkaListenerAnnotation, key.factoryFromKafkaListenerAnnotation)
						&& Objects.equals(this.config, key.config);
			}

			@Override
			public int hashCode() {
				return Objects.hash(this.factoryFromKafkaListenerAnnotation, this.config);
			}
		}

	}

}
