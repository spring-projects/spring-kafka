/*
 * Copyright 2018-2022 the original author or authors.
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.kafka.annotation.RetryTopicConfigurationProvider;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaOperations;

/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Fabio da Silva Jr.
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class RetryTopicConfigurationProviderTests {

	private ConfigurableListableBeanFactory beanFactory;

	{
		this.beanFactory = mock(ConfigurableListableBeanFactory.class);
		willAnswer(invoc -> {
			return invoc.getArgument(0);
		}).given(this.beanFactory).resolveEmbeddedValue(anyString());
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
	RetryableTopic annotation;

	@Mock
	KafkaOperations<?, ?> kafkaOperations;

	@Mock
	RetryTopicConfiguration retryTopicConfiguration;

	@Mock
	RetryTopicConfiguration retryTopicConfiguration2;

	@Test
	void shouldProvideFromAnnotation() {

		// setup
		willReturn(kafkaOperations).given(beanFactory).getBean("retryTopicDefaultKafkaTemplate", KafkaOperations.class);

		// given
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(beanFactory);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, annotatedMethod, bean);

		// then
		then(this.beanFactory).should(times(0)).getBeansOfType(RetryTopicConfiguration.class);

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

		// then
		then(this.beanFactory).should(times(1)).getBeansOfType(RetryTopicConfiguration.class);
		assertThat(configuration).isEqualTo(retryTopicConfiguration);

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

		// then
		then(this.beanFactory).should(times(1)).getBeansOfType(RetryTopicConfiguration.class);
		assertThat(configuration).isNull();

	}

	@Test
	void shouldProvideFromMetaAnnotation() {

		// setup
		willReturn(kafkaOperations).given(beanFactory).getBean("retryTopicDefaultKafkaTemplate", KafkaOperations.class);

		// given
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(beanFactory);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, metaAnnotatedMethod, bean);

		// then
		then(this.beanFactory).should(times(0)).getBeansOfType(RetryTopicConfiguration.class);
	}

	@Test
	void shouldNotConfigureIfBeanFactoryNull() {

		// given
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(null);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, nonAnnotatedMethod, bean);

		// then
		assertThat(configuration).isNull();

	}

	@RetryableTopic
	public void annotatedMethod() {
		// NoOps
	}

	public void nonAnnotatedMethod() {
		// NoOps
	}

	@Target({ElementType.METHOD})
	@Retention(RetentionPolicy.RUNTIME)
	@RetryableTopic
	static @interface MetaAnnotatedRetryableTopic {
	}

	@MetaAnnotatedRetryableTopic
	public void metaAnnotatedMethod() {
		// NoOps
	}
}
