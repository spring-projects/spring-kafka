/*
 * Copyright 2017-2019 the original author or authors.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Sela Lerer
 * @since 2.4
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class KafkaListenerUseAcknowledgementWithWrongAckModeTests {

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;
	@Autowired
	private KafkaListenerUseAcknowledgementWithWrongAckModeTests.Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Autowired
	private ErrorHandler errorHandler;

	@Test
	public void discardRemainingRecordsFromPollAndSeek() throws Exception {
		synchronized (config.somethingHappenedNotification) {
			config.somethingHappenedNotification.wait(10 * 1000);
		}

		assertThat(errorHandler.getError()).getRootCause().hasMessageContaining("spring.kafka.listener.ack-mode=manual");
	}

	@Configuration
	@EnableKafka
	public static class Config {

		private final Object somethingHappenedNotification = new Object();

		@KafkaListener(topics = "foo", groupId = "grp", errorHandler = "errorHandler")
		public void foo(@Payload ConsumerRecord<String, String> in, Acknowledgment ack) {
			synchronized (somethingHappenedNotification) {
				// Received message
				somethingHappenedNotification.notifyAll();
			}
			ack.acknowledge();
		}

		@Bean
		public ErrorHandler errorHandler() {
			return new ErrorHandler(somethingHappenedNotification);
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			final Consumer consumer = consumer();
			given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
					.willReturn(consumer);
			return consumerFactory;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public Consumer consumer() {
			final Consumer consumer = mock(Consumer.class);
			final TopicPartition topicPartition0 = new TopicPartition("foo", 0);
			Map<TopicPartition, List<ConsumerRecord>> records1 = new LinkedHashMap<>();
			records1.put(topicPartition0, Arrays.asList(
					new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "foo"),
					new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "bar")));
			willAnswer(i -> new ConsumerRecords(records1))
					.given(consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
			return consumer;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			factory.setErrorHandler(new SeekToCurrentErrorHandler());
			factory.getContainerProperties().setMissingTopicsFatal(false);
			return factory;
		}

	}

	public static class ErrorHandler implements KafkaListenerErrorHandler {

		private final Object notifyWhenError;
		private ListenerExecutionFailedException error;

		public ErrorHandler(Object notifyWhenError) {
			this.notifyWhenError = notifyWhenError;
		}

		@Override
		public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
			synchronized (notifyWhenError) {
				error = exception;
				notifyWhenError.notifyAll();
			}
			throw exception;
		}

		public ListenerExecutionFailedException getError() {
			return error;
		}
	}
}
