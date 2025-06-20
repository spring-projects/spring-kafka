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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.RetryTopicConfigurationProvider;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaOperations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Fabio da Silva Jr.
 * @author Wang Zhiyang
 *
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class RetryTopicConfigurationProviderTests {

	private ConfigurableListableBeanFactory beanFactory;

	{
		this.beanFactory = mock(ConfigurableListableBeanFactory.class);
		willAnswer(invoc -> invoc.getArgument(0)).given(this.beanFactory).resolveEmbeddedValue(anyString());
	}

	private final String[] topics = {"topic1", "topic2"};

	private final Method annotatedMethod = getAnnotatedMethod("annotatedMethod");

	private final Method nonAnnotatedMethod = getAnnotatedMethod("nonAnnotatedMethod");

	private final Method metaAnnotatedMethod = getAnnotatedMethod("metaAnnotatedMethod");

	private Method getAnnotatedMethod(String methodName) {
		try {
			return  this.getClass().getDeclaredMethod(methodName);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Mock
	Object bean;

	@Mock
	KafkaOperations<?, ?> kafkaOperations;

	@Mock
	RetryTopicConfiguration retryTopicConfiguration;

	@Test
	void shouldProvideFromAnnotation() {

		// setup
		willReturn(kafkaOperations).given(beanFactory).getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class);

		// given
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(beanFactory);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, annotatedMethod, bean);
		RetryTopicConfiguration configurationFromClass = provider
				.findRetryConfigurationFor(topics, null, AnnotatedClass.class, bean);

		// then
		then(this.beanFactory).should(times(0)).getBeansOfType(RetryTopicConfiguration.class);
		assertThat(configuration).isNotNull();
		assertThat(configurationFromClass).isNotNull();
	}

	@Test
	void shouldProvideFromBeanFactory() {

		// setup
		willReturn(Collections.singletonMap("retryTopicConfiguration", retryTopicConfiguration))
				.given(this.beanFactory).getBeansOfType(RetryTopicConfiguration.class);
		given(retryTopicConfiguration.hasConfigurationForTopics(topics)).willReturn(true);

		// given
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(beanFactory);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, nonAnnotatedMethod, bean);
		RetryTopicConfiguration configurationFromClass = provider
				.findRetryConfigurationFor(topics, null, NonAnnotatedClass.class, bean);

		// then
		then(this.beanFactory).should(times(2)).getBeansOfType(RetryTopicConfiguration.class);
		assertThat(configuration).isEqualTo(retryTopicConfiguration);
		assertThat(configurationFromClass).isEqualTo(retryTopicConfiguration);

	}

	@Test
	void shouldFindNone() {

		// setup
		willReturn(Collections.singletonMap("retryTopicConfiguration", retryTopicConfiguration))
				.given(this.beanFactory).getBeansOfType(RetryTopicConfiguration.class);
		given(retryTopicConfiguration.hasConfigurationForTopics(topics)).willReturn(false);

		// given
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(beanFactory);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, nonAnnotatedMethod, bean);
		RetryTopicConfiguration configurationFromClass = provider
				.findRetryConfigurationFor(topics, null, NonAnnotatedClass.class, bean);

		// then
		then(this.beanFactory).should(times(2)).getBeansOfType(RetryTopicConfiguration.class);
		assertThat(configuration).isNull();
		assertThat(configurationFromClass).isNull();

	}

	@Test
	void shouldProvideFromMetaAnnotation() {

		// setup
		willReturn(kafkaOperations).given(beanFactory).getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class);

		// given
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(beanFactory);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, metaAnnotatedMethod, bean);
		RetryTopicConfiguration configurationFromClass = provider
				.findRetryConfigurationFor(topics, null, MetaAnnotatedClass.class, bean);

		// then
		then(this.beanFactory).should(times(0)).getBeansOfType(RetryTopicConfiguration.class);
		assertThat(configuration).isNotNull();
		assertThat(configuration.getConcurrency()).isEqualTo(3);
		assertThat(configurationFromClass).isNotNull();
		assertThat(configurationFromClass.getConcurrency()).isEqualTo(3);

	}

	@Test
	void shouldNotConfigureIfBeanFactoryNull() {

		// given
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(null);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, nonAnnotatedMethod, bean);
		RetryTopicConfiguration configurationFromClass
				= provider.findRetryConfigurationFor(topics, null, NonAnnotatedClass.class, bean);

		// then
		assertThat(configuration).isNull();
		assertThat(configurationFromClass).isNull();

	}

	@RetryableTopic
	public void annotatedMethod() {
		// NoOps
	}

	public void nonAnnotatedMethod() {
		// NoOps
	}

	@Retention(RetentionPolicy.RUNTIME)
	@RetryableTopic
	@interface MetaAnnotatedRetryableTopic {
		@AliasFor(attribute = "concurrency", annotation = RetryableTopic.class)
		String parallelism() default "3";
	}

	@MetaAnnotatedRetryableTopic
	public void metaAnnotatedMethod() {
		// NoOps
	}

	@RetryableTopic
	public static class AnnotatedClass {
		// NoOps
	}

	public static class NonAnnotatedClass {
		// NoOps
	}

	@MetaAnnotatedRetryableTopic
	public static class MetaAnnotatedClass {
		// NoOps
	}

}
