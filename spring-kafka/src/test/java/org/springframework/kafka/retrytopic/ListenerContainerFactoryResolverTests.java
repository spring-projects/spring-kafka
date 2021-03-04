/*
 * Copyright 2018-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class ListenerContainerFactoryResolverTests {

	@Mock
	private BeanFactory beanFactory;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromKafkaListenerAnnotation;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromRetryTopicConfiguration;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromBeanName;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromOtherBeanName;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromDefaultBeanName;

	private final static String factoryName = "testListenerContainerFactory";
	private final static String otherFactoryName = "otherTestListenerContainerFactory";
	private final static String defaultFactoryBeanName = "defaultTestListenerContainerFactory";

	@Test
	void shouldResolveWithKLAFactoryForMainEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver
				.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver
				.resolveFactoryForMainEndpoint(factoryFromKafkaListenerAnnotation, defaultFactoryBeanName, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromKafkaListenerAnnotation);
	}

	@Test
	void shouldResolveWithRTConfigurationFactoryForMainEndpointIfKLAAbsent() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver
				.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver
				.resolveFactoryForMainEndpoint(null, defaultFactoryBeanName, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromRetryTopicConfiguration);
	}

	@Test
	void shouldResolveFromBeanNameForMainEndpoint() {

		// setup
		given(beanFactory.getBean(factoryName, ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver
						.resolveFactoryForMainEndpoint(null, null, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromBeanName);
	}

	@Test
	void shouldResolveFromDefaultFactoryBeanNameForMainEndpoint() {

		// setup
		given(beanFactory.getBean(defaultFactoryBeanName, ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null,
						defaultFactoryBeanName, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromBeanName);
	}

	@Test
	void shouldResolveFromDefaultBeanNameForMainEndpoint() {

		// setup
		given(beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_LISTENER_FACTORY_BEAN_NAME,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, null, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromBeanName);
	}

	@Test
	void shouldFailIfNoneResolvedForMainEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		// given
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// then
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> listenerContainerFactoryResolver
				.resolveFactoryForMainEndpoint(null, null, configuration));
	}

	@Test
	void shouldResolveWithRetryTopicConfigurationFactoryForRetryEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver
				.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver
				.resolveFactoryForRetryEndpoint(factoryFromKafkaListenerAnnotation, null, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromRetryTopicConfiguration);
	}

	@Test
	void shouldResolveFromBeanNameForRetryEndpoint() {

		// setup
		given(beanFactory.getBean(factoryName, ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver
				.Configuration(null, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver
				.resolveFactoryForRetryEndpoint(factoryFromKafkaListenerAnnotation, null, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromBeanName);
	}

	@Test
	void shouldResolveWithKLAFactoryForRetryEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(factoryFromKafkaListenerAnnotation, null, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromKafkaListenerAnnotation);
	}

	@Test
	void shouldResolveWithDefaultKLAFactoryForRetryEndpoint() {

		// setup
		given(beanFactory.getBean(defaultFactoryBeanName, ConcurrentKafkaListenerContainerFactory.class))
				.willReturn(factoryFromDefaultBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null,
						defaultFactoryBeanName, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromDefaultBeanName);
	}

	@Test
	void shouldResolveFromDefaultBeanNameForRetryEndpoint() {

		// setup
		given(beanFactory.getBean(defaultFactoryBeanName,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null,
						defaultFactoryBeanName, configuration);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromBeanName);
	}

	@Test
	void shouldFailIfNoneResolvedForRetryEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		// given
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// then
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null, defaultFactoryBeanName, configuration));
	}

	@Test
	void shouldGetFromCacheForMainEndpont() {

		// setup
		given(beanFactory.getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		ListenerContainerFactoryResolver.Configuration configuration2 =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null,
						null, configuration);

		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory2 =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null,
						null, configuration2);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromBeanName);
		assertThat(resolvedFactory2).isEqualTo(factoryFromBeanName);
		then(beanFactory).should(times(1)).getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class);
	}

	@Test
	void shouldGetFromCacheForRetryEndpoint() {

		// setup
		given(beanFactory.getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		ListenerContainerFactoryResolver.Configuration configuration2 =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null,
						null, configuration);

		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory2 =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null,
						null, configuration2);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromBeanName);
		assertThat(resolvedFactory2).isEqualTo(factoryFromBeanName);
		then(beanFactory).should(times(1)).getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class);
	}

	@Test
	void shouldNotGetFromCacheForMainEndpoint() {

		// setup
		given(beanFactory.getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromBeanName);
		given(beanFactory.getBean(otherFactoryName,
				ConcurrentKafkaListenerContainerFactory.class)).willReturn(factoryFromOtherBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		ListenerContainerFactoryResolver.Configuration configuration2 =
				new ListenerContainerFactoryResolver.Configuration(null, otherFactoryName);

		// given
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null,
						null, configuration);

		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory2 =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null,
						null, configuration2);

		// then
		assertThat(resolvedFactory).isEqualTo(factoryFromBeanName);
		assertThat(resolvedFactory2).isEqualTo(factoryFromOtherBeanName);
		then(beanFactory).should(times(1)).getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class);
		then(beanFactory).should(times(1)).getBean(otherFactoryName,
				ConcurrentKafkaListenerContainerFactory.class);
	}

	@Test
	void shouldGetFromCacheWithSameConfiguration() {

		// setup
		ListenerContainerFactoryResolver.Cache cache = new ListenerContainerFactoryResolver.Cache();
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver
				.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		cache.addIfAbsent(factoryFromKafkaListenerAnnotation, configuration, factoryFromDefaultBeanName);

		// then
		assertThat(factoryFromDefaultBeanName).isEqualTo(cache.fromCache(factoryFromKafkaListenerAnnotation, configuration));
	}

	@Test
	void shouldGetFromCacheWithEqualConfiguration() {

		// setup
		ListenerContainerFactoryResolver.Cache cache = new ListenerContainerFactoryResolver.Cache();
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver
				.Configuration(factoryFromRetryTopicConfiguration, factoryName);
		ListenerContainerFactoryResolver.Configuration configuration2 = new ListenerContainerFactoryResolver
				.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// given
		cache.addIfAbsent(factoryFromKafkaListenerAnnotation, configuration, factoryFromDefaultBeanName);

		// then
		assertThat(factoryFromDefaultBeanName).isEqualTo(cache.fromCache(factoryFromKafkaListenerAnnotation, configuration2));
	}

	@Test
	void shouldNotGetFromCacheWithDifferentConfiguration() {

		// setup
		ListenerContainerFactoryResolver.Cache cache = new ListenerContainerFactoryResolver.Cache();
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver
				.Configuration(factoryFromRetryTopicConfiguration, factoryName);
		ListenerContainerFactoryResolver.Configuration configuration2 = new ListenerContainerFactoryResolver
				.Configuration(factoryFromOtherBeanName, factoryName);

		// given
		cache.addIfAbsent(factoryFromKafkaListenerAnnotation, configuration, factoryFromDefaultBeanName);

		// then
		assertThat(cache.fromCache(factoryFromKafkaListenerAnnotation, configuration2)).isNull();
	}
}
