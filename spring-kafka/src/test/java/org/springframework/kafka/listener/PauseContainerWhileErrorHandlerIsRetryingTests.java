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

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.spy;

/**
 * @author Antonio Tomac
 * @author Gary Russell
 * @since 2.9
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = "foo", partitions = 1)
public class PauseContainerWhileErrorHandlerIsRetryingTests {

	private static final LogAccessor log = new LogAccessor(LogFactory.getLog(PauseContainerWhileErrorHandlerIsRetryingTests.class));

	private static void log(String message) {
		log.error(message);
	}

	@Autowired
	private Config setup;

	@Test
	public void provokeRetriesTriggerPauseThenResume() throws InterruptedException {
		setup.produce(1, 2);  //normally processed
		await("for first 2 records")
				.atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> assertThat(setup.received).as("received").contains("1", "2"));
		await().untilAsserted(() -> assertThat(setup.processed).as("processed").contains("1", "2"));

		setup.triggerPause.set(true);
		log("enable listener throwing");
		setup.failing.set(true);
		setup.produce(3, 4, 5);    //could loose those

		await("for next 3 records")
				.atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> assertThat(setup.received)
						.as("received")
						.hasSizeGreaterThan(2));
		await().untilAsserted(() -> assertThat(setup.processed).as("processed").hasSize(2));

		setup.triggerPause.set(false);
		setup.resumeContainer();

		log("disable listener throwing");
		setup.failing.set(false);
		setup.produce(6, 7, 8, 9);

		await("for last 4 records")
				.atMost(Duration.ofSeconds(10))
				.untilAsserted(() -> assertThat(setup.received)
						.as("received - all")
						.contains("1", "2", "3", "4", "5", "6", "7", "8", "9"));
		await().untilAsserted(() -> assertThat(setup.processed)
				.as("processed all - not loosing 3, 4, 5")
				.contains("1", "2", "3", "4", "5", "6", "7", "8", "9"));
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Autowired
		KafkaListenerEndpointRegistry registry;

		@Autowired
		EmbeddedKafkaBroker embeddedKafkaBroker;

		final Set<String> received = new LinkedHashSet<>();

		final Set<String> processed = new LinkedHashSet<>();

		final AtomicBoolean failing = new AtomicBoolean(false);

		final AtomicBoolean triggerPause = new AtomicBoolean(false);

		void resumeContainer() {
			log("resuming...");
			registry.getListenerContainer("id").resume();    //NOSONAR
			log("resumed");
		}

		void pauseContainer() {
			log("pausing...");
			registry.getListenerContainer("id").pause();    //NOSONAR
			log("paused");
		}

		void produce(int... records) {
			ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
			try (Producer<Integer, String> producer = pf.createProducer()) {
				for (int record : records) {
					log("producing message: " + record);
					producer.send(new ProducerRecord<>("foo", record, Integer.toString(record)));
				}
				producer.flush();
			}
		}

		@KafkaListener(id = "id", groupId = "grp", topics = "foo")
		public void process(List<String> batch, Acknowledgment acknowledgment) {
			batch.forEach((msg) -> {
				if (!received.contains(msg)) {
					log("Got new message: " + msg);
				}
				received.add(msg);
			});
			received.addAll(batch);
			if (failing.get()) {
				throw new RuntimeException("ooops");
			}
			batch.forEach((msg) -> {
				if (!processed.contains(msg)) {
					log("Processed new message: " + msg);
				}
				processed.add(msg);
			});
			acknowledgment.acknowledge();
		}

		/**
		 * Call {@link #pauseContainer()} is timed during {@link KafkaMessageListenerContainer.ListenerConsumer#polling}
		 * is being `true`, but after Consumer's check if it had been woken up.
		 * Problem depends the fact that very next call {@link Consumer#poll(Duration)}
		 * will throw {@link org.apache.kafka.common.errors.WakeupException}
		 */
		@SuppressWarnings({"rawtypes"})
		private Consumer makePausingAfterPollConsumer(Consumer delegate) {
			Consumer spied = spy(delegate);
			willAnswer((call) -> {
				Duration duration = call.getArgument(0, Duration.class);
				ConsumerRecords records = delegate.poll(duration);
				if (!duration.isZero() && triggerPause.get()) {
					pauseContainer();
				}
				return records;
			}).given(spied).poll(any());
			return spied;
		}

		@SuppressWarnings({"rawtypes"})
		private ConsumerFactory makePausingAfterPollConsumerFactory(ConsumerFactory delegate) {
			ConsumerFactory spied = spy(delegate);
			willAnswer((invocation -> {
				Consumer consumerDelegate = delegate.createConsumer(
						invocation.getArgument(0, String.class),
						invocation.getArgument(1, String.class),
						invocation.getArgument(2, String.class),
						invocation.getArgument(3, Properties.class)
				);
				return makePausingAfterPollConsumer(consumerDelegate);
			})).given(spied).createConsumer(any(), any(), any(), any());
			return spied;
		}

		@SuppressWarnings({"rawtypes", "unchecked"})
		@Bean
		ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
			DefaultKafkaConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(
					KafkaTestUtils.consumerProps(embeddedKafkaBroker, "grp", false)
			);
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setBatchListener(true);
			factory.setConsumerFactory(makePausingAfterPollConsumerFactory(consumerFactory));
			factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
			factory.getContainerProperties().setPollTimeoutWhilePaused(Duration.ZERO);
			DefaultErrorHandler eh = new DefaultErrorHandler(new FixedBackOff(100, Long.MAX_VALUE));
			eh.setSeekAfterError(true);
			factory.setCommonErrorHandler(eh);
			return factory;
		}

	}

}
