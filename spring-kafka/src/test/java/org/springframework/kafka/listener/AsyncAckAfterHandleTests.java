/*
 * Copyright 2023-present the original author or authors.
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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = "asaah")
public class AsyncAckAfterHandleTests {

	@Test
	void testAckAfterHandlerAsync(@Autowired
	Config config, @Autowired
	KafkaTemplate<Integer, String> template)
			throws InterruptedException {

		for (int i = 0; i < 6; i++) {
			template.send("asaah", 0, null, "message contents");
		}
		assertThat(config.latch.await(10, TimeUnit.SECONDS))
				.describedAs("CountDownLatch.count=%d", config.latch.getCount())
				.isTrue();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		private final CountDownLatch latch = new CountDownLatch(6);

		@KafkaListener(id = "asaah.id", topics = "asaah")
		public void onTestTopic(final ConsumerRecord<byte[], byte[]> record,
				final Acknowledgment acknowledgment) {
			accept(record, acknowledgment);
		}

		private void accept(final ConsumerRecord<byte[], byte[]> record,
				final Acknowledgment acknowledgment) {
			if (record.offset() == 1) {
				throw new RuntimeException("Exception for error handler");
			}
			else {
				this.latch.countDown();
				acknowledgment.acknowledge();
			}
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			factory.setCommonErrorHandler(new MreErrorHandler());
			factory.getContainerProperties().setAsyncAcks(true);
			factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
			factory.getContainerProperties().setCommitLogLevel(LogIfLevelEnabled.Level.TRACE);
			return factory;
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "asaac.grp", false);
			props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);
			return new DefaultKafkaConsumerFactory<>(
					props);
		}

		@Bean
		ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.producerProps(broker);
			props.put(ProducerConfig.LINGER_MS_CONFIG, 100L);
			return new DefaultKafkaProducerFactory<>(props);
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		public class MreErrorHandler implements CommonErrorHandler {

			@Override
			public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
					MessageListenerContainer container) {
				Config.this.latch.countDown();
				return true;
			}
		}

	}

}
