/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

/**
 * @author Asi Bross
 *
 * @since 2.3.5
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class KafkaListenerEndpointRegistryLifecycleTests {

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Test
	public void lifecycleTest() throws InterruptedException, ExecutionException, TimeoutException {
		// Registry is started automatically by the application context
		assertThat(registry.isRunning()).isTrue();

		this.registry.stop();
		assertThat(registry.isRunning()).isFalse();

		this.registry.start();
		assertThat(registry.isRunning()).isTrue();

		CompletableFuture<Boolean> isRunning = new CompletableFuture<>();
		this.registry.stop(() -> isRunning.complete(this.registry.isRunning()));
		assertThat(isRunning.get(1, TimeUnit.SECONDS)).isFalse();
	}

	@KafkaListener(topics = "foo", groupId = "bar")
	public void listen() {
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(mock(ConsumerFactory.class, RETURNS_DEEP_STUBS));
			return factory;
		}

	}

}
