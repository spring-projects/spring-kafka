/*
 * Copyright 2025-present the original author or authors.
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.AbstractKafkaListenerEndpoint;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

/**
 * @author Sanghyeok An
 *
 * @since 4.0.0
 */

class KafkaListenerAnnotationBeanPostProcessorTests {

	@Test
	void ctx_should_be_fail_to_register_bean_when_no_listener_methods_exist() {

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.register(TestConfig.class);

		assertThatExceptionOfType(BeanCreationException.class)
				.isThrownBy(ctx::refresh)
				.withRootCauseInstanceOf(IllegalStateException.class)
				.withMessageContaining("No Kafka listener methods in bean:")
				.withMessageContaining("NoHandlerMethodListener");

	}

	@Test
	void shouldRejectMultipleTopicSpecifications() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.register(MultipleTopicSpecConfig.class);

		assertThatExceptionOfType(BeanCreationException.class)
				.isThrownBy(ctx::refresh)
				.withRootCauseInstanceOf(IllegalStateException.class)
				.withMessageContaining("Only one of @Topic or @TopicPartition or @TopicPattern must be provided");
	}

	@Test
	void shouldAllowNoTopicSpecificationForProgrammaticResolution() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.register(NoTopicSpecConfig.class);

		assertThatNoException().isThrownBy(ctx::refresh);
		ctx.close();
	}

	@EnableKafka
	@Configuration
	static class TestConfig {

		@Component
		@KafkaListener
		static class NoHandlerMethodListener {

			public void listen(String message) {
			}
		}

	}

	@EnableKafka
	@Configuration
	static class MultipleTopicSpecConfig {

		@SuppressWarnings("unchecked")
		@Bean
		ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<String, Object> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(mock(ConsumerFactory.class));
			return factory;
		}

		@Component
		static class MultipleTopicSpecListener {

			@KafkaListener(topics = "topic1", topicPattern = "topic.*")
			public void listen(String message) {
			}
		}

	}

	@EnableKafka
	@Configuration
	static class NoTopicSpecConfig {

		@SuppressWarnings("unchecked")
		@Bean
		TopicResolvingContainerFactory myFactory() {
			TopicResolvingContainerFactory factory = new TopicResolvingContainerFactory();
			factory.setConsumerFactory(mock(ConsumerFactory.class));
			factory.setAutoStartup(false);
			return factory;
		}

		@Component
		static class MetaAnnotatedListener {

			@MyCustomKafkaListener
			public void listen(String message) {
			}
		}

	}

	@Target(ElementType.METHOD)
	@Retention(RetentionPolicy.RUNTIME)
	@KafkaListener(containerFactory = "myFactory")
	@interface MyCustomKafkaListener {
	}

	/**
	 * Custom container factory that resolves topics programmatically,
	 * simulating the use case where a meta-annotated @KafkaListener
	 * does not specify topics in the annotation.
	 */
	static class TopicResolvingContainerFactory extends ConcurrentKafkaListenerContainerFactory<String, Object> {

		@Override
		public ConcurrentMessageListenerContainer<String, Object> createListenerContainer(
				KafkaListenerEndpoint endpoint) {

			if (endpoint instanceof AbstractKafkaListenerEndpoint<?, ?> akle) {
				akle.setTopics("programmatically-resolved-topic");
			}
			return super.createListenerContainer(endpoint);
		}

	}

}
