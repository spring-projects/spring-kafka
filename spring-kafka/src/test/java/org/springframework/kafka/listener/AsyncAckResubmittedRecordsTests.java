/*
 * Copyright 2026-present the original author or authors.
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Sharang Gupta
 *
 * @since 4.2
 */
@EmbeddedKafka(topics = AsyncAckResubmittedRecordsTests.TOPIC, partitions = 2)
class AsyncAckResubmittedRecordsTests {

	static final String TOPIC = "asyncAckResubmittedRecords";

	private static final String GROUP = "asyncAckResubmittedRecords";

	@Test
	void pendingAckOnAnotherPartitionSurvivesResubmittedRecords() throws Exception {
		EmbeddedKafkaBroker broker = EmbeddedKafkaCondition.getBroker();
		KafkaTemplate<Integer, String> template =
				new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker)));
		for (int partition = 0; partition < 2; partition++) {
			template.send(TOPIC, partition, null, "first");
			template.send(TOPIC, partition, null, "second");
		}
		template.flush();

		ContainerProperties containerProps = new ContainerProperties(TOPIC);
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		containerProps.setAsyncAcks(true);
		List<String> deliveries = new CopyOnWriteArrayList<>();
		AtomicInteger pendingPartition = new AtomicInteger(-1);
		AtomicReference<Acknowledgment> pendingAck = new AtomicReference<>();
		AtomicBoolean failedOnce = new AtomicBoolean();
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (record, ack) -> {
			deliveries.add(record.partition() + ":" + record.value());
			// Whichever partition comes first keeps its first record unacknowledged,
			// the other partition fails once on its first record.
			if (pendingPartition.compareAndSet(-1, record.partition())) {
				pendingAck.set(ack);
				return;
			}
			if (record.partition() != pendingPartition.get() && failedOnce.compareAndSet(false, true)) {
				throw new IllegalStateException("The record fails once");
			}
			ack.acknowledge();
		});
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(broker, GROUP, false);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(consumerProps), containerProps);
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 1L));
		errorHandler.setSeekAfterError(false);
		container.setCommonErrorHandler(errorHandler);
		container.start();
		try {
			await().atMost(Duration.ofSeconds(30))
					.untilAsserted(() -> assertThat(committedOffset(broker, 1 - pendingPartition.get())).isEqualTo(2L));
			// The other partition was resubmitted after its failure while the first record waited for its ack
			assertThat(deliveries).hasSize(5);

			pendingAck.get().acknowledge();

			await().atMost(Duration.ofSeconds(30))
					.untilAsserted(() -> assertThat(committedOffset(broker, pendingPartition.get())).isEqualTo(2L));
		}
		finally {
			container.stop();
			template.destroy();
		}
	}

	private static long committedOffset(EmbeddedKafkaBroker broker, int partition) throws Exception {
		OffsetAndMetadata offset = KafkaTestUtils.getCurrentOffset(broker.getBrokersAsString(), GROUP, TOPIC, partition);
		return offset != null ? offset.offset() : -1;
	}

}
