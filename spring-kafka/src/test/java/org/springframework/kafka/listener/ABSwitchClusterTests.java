/*
 * Copyright 2020 the original author or authors.
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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ABSwitchCluster;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.6
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class ABSwitchClusterTests {

	@Test
	void testSwitch(@Autowired Config config, @Autowired ABSwitchCluster switcher,
			@Autowired KafkaListenerEndpointRegistry registry) throws InterruptedException {

		assertThat(config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("foo");
		registry.stop();
		switcher.secondary();
		config.latch = new CountDownLatch(1);
		registry.start();
		assertThat(config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("bar");
		registry.stop();
		switcher.primary();
		config.latch = new CountDownLatch(1);
		registry.start();
		assertThat(config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("foo");
		registry.stop();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		private static final ConsumerRecords<Object, Object> EMPTY = new ConsumerRecords<>(Collections.emptyMap());

		volatile CountDownLatch latch = new CountDownLatch(1);

		volatile Map<String, Object> props;

		@Bean
		public DefaultKafkaConsumerFactory<Object, Object> cf() {
			Map<String, Object> configs = new HashMap<>();
			DefaultKafkaConsumerFactory<Object, Object> cf = new DefaultKafkaConsumerFactory<Object, Object>(configs) {

				@Override
				protected Consumer<Object, Object> createRawConsumer(Map<String, Object> configProps) {
					@SuppressWarnings("unchecked")
					Consumer<Object, Object> consumer = mock(Consumer.class);
					willAnswer(inv -> {
						Thread.sleep(100);
						return EMPTY;
					}).given(consumer).poll(any());
					Config.this.props = configProps;
					Config.this.latch.countDown();
					return consumer;
				}

			};
			cf.setBootstrapServersSupplier(switcher());
			return cf;
		}

		@Bean
		public ABSwitchCluster switcher() {
			return new ABSwitchCluster("foo", "bar");
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Object, Object> cf = new ConcurrentKafkaListenerContainerFactory<>();
			cf.setConsumerFactory(cf());
			return cf;
		}

		@KafkaListener(id = "ab", topics = "foo")
		public void listen() {
		}

	}

}
