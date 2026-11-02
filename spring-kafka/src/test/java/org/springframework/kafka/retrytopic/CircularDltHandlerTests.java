/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

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

import static org.mockito.Mockito.mock;

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
