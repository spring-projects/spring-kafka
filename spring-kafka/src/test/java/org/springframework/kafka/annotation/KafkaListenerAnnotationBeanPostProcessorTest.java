/*
 * Copyright 2017-2025 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Sanghyeok An
 *
 * @since 4.0.0
 */

class KafkaListenerAnnotationBeanPostProcessorTest {

	@Test
	void ctx_should_be_fail_to_register_bean_when_no_listener_methods_exist() {
		// GIVEN
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.register(TestConfig.class);

		// GIVEN - expected
		Class<?> expectedErrorType = BeanCreationException.class;
		String expectedErrorMsg =
				"Error creating bean with name 'org.springframework.kafka.annotation."
				+ "KafkaListenerAnnotationBeanPostProcessorTest$TestConfig$BuggyListener': "
				+ "No kafka listener methods found on bean type: class org.springframework.kafka"
				+ ".annotation.KafkaListenerAnnotationBeanPostProcessorTest$TestConfig$BuggyListener";

		// WHEN + THEN
		assertThatThrownBy(ctx::refresh)
				.isInstanceOf(expectedErrorType)
				.hasMessage(expectedErrorMsg);
	}

	@Configuration
	static class TestConfig {

		@Bean
		public KafkaListenerAnnotationBeanPostProcessor<Object, Object> kafkaListenerAnnotationBeanPostProcessor() {
			return new KafkaListenerAnnotationBeanPostProcessor<>();
		}

		@Component
		@KafkaListener
		static class BuggyListener {

			public void listen(String message) {
				return;
			}
		}

	}

}
