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

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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

}
