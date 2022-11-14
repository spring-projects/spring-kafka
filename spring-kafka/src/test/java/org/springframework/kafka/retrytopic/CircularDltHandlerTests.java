/*
 * Copyright 2021-2022 the original author or authors.
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

import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * @author Gary Russell
 * @since 2.8
 *
 */
public class CircularDltHandlerTests {

	@Test
	void contextLoads() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(Config.class);
		context.setAllowCircularReferences(false);
		context.refresh();
	}

	@Configuration
	@EnableKafka
	public static class Config extends RetryTopicConfigurationSupport {

		@SuppressWarnings("unchecked")
		@Bean
		ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(mock(ConsumerFactory.class));
			return factory;
		}

		@Bean
		RetryTopicConfiguration retryConfig() {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.maxAttempts(1)
					.dltHandlerMethod("listener", "dlt")
					.create(mock(KafkaTemplate.class));
		}

		@Bean
		Listener listener() {
			return new Listener();
		}

		@Bean
		TaskScheduler sched() {
			return new ThreadPoolTaskScheduler();
		}

	}

	public static class Listener {

		@KafkaListener(id = "test", topics = "test", autoStartup = "false")
		void listen(String in) {
		}

		public void dlt(String in) {
		}

	}

}
