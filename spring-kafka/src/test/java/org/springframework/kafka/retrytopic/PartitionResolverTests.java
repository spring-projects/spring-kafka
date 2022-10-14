/*
 * Copyright 2022 the original author or authors.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.9.2
 *
 */
@DirtiesContext
@SpringJUnitConfig
@EmbeddedKafka(topics = "partition.resolver.tests")
public class PartitionResolverTests extends AbstractRetryTopicIntegrationTests {

	@Test
	void testNullPartition(@Autowired KafkaOperations<Integer, String> template,
			@Autowired EmbeddedKafkaBroker broker, @Autowired Config config) throws InterruptedException {

		Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
		KafkaTemplate<Integer, String> kt = new KafkaTemplate<>(pf);
		kt.send("partition.resolver.tests", 1, null, "test");
		assertThat(config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		@SuppressWarnings("unchecked")
		ArgumentCaptor<ProducerRecord<Integer, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(template).send(captor.capture());
		assertThat(captor.getValue().partition()).isNull();
	}

	@EnableKafka
	@Configuration
	public static class Config extends RetryTopicConfigurationSupport {

		final CountDownLatch latch = new CountDownLatch(1);

		@Override
		protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
			return dlprf -> dlprf.setPartitionResolver((cr, nextTopic) -> null);
		}

		@Bean
		RetryTopicConfiguration myRetryTopic(KafkaOperations<Integer, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.create(template);
		}

		@SuppressWarnings("unchecked")
		@Bean
		KafkaOperations<Integer, String> template() {
			KafkaOperations<Integer, String> mock = mock(KafkaOperations.class);
			CompletableFuture<SendResult<Integer, String>> future = new CompletableFuture<>();
			future.complete(mock(SendResult.class));
			willAnswer(inv -> {
				latch.countDown();
				return future;
			}).given(mock).send(any(ProducerRecord.class));
			return mock;
		}

		@KafkaListener(topics = "partition.resolver.tests")
		void listen(String in) {
			throw new RuntimeException("test");
		}

		@Bean
		ConsumerFactory<Integer, String> cf(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.consumerProps("prt", "false", broker);
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> cf) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			return factory;
		}

		@Bean
		TaskScheduler taskScheduler() {
			return new ThreadPoolTaskScheduler();
		}

	}

}
