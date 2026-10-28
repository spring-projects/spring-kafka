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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.ContainerProperties.AssignmentCommitOption;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.TopicPartitionOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Sharang Gupta
 *
 * @since 4.2
 */
public class BatchNackInterceptedRecordsTests {

	private static final TopicPartition TP0 = new TopicPartition("foo", 0);

	@Test
	void nackIndexRefersToTheRecordsReturnedByTheBatchInterceptor() {
		ConsumerFactory<Integer, String> cf = mock();
		Consumer<Integer, String> consumer = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		ConsumerRecord<Integer, String> r0 = new ConsumerRecord<>("foo", 0, 0L, 0, "r0");
		ConsumerRecord<Integer, String> r1 = new ConsumerRecord<>("foo", 0, 1L, 0, "r1");
		ConsumerRecord<Integer, String> r2 = new ConsumerRecord<>("foo", 0, 2L, 0, "r2");
		AtomicBoolean first = new AtomicBoolean(true);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			if (first.getAndSet(false)) {
				return new ConsumerRecords<>(Map.of(TP0, List.of(r0, r1, r2)), Map.of());
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});
		given(consumer.paused()).willReturn(Collections.emptySet());
		List<Long> seeks = new CopyOnWriteArrayList<>();
		willAnswer(i -> seeks.add(i.getArgument(1))).given(consumer).seek(any(TopicPartition.class), any(Long.class));

		ContainerProperties containerProps = new ContainerProperties(new TopicPartitionOffset("foo", 0));
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.MANUAL);
		containerProps.setAssignmentCommitOption(AssignmentCommitOption.NEVER);
		AtomicBoolean nacked = new AtomicBoolean();
		containerProps.setMessageListener((BatchAcknowledgingMessageListener<Integer, String>) (records, ack) -> {
			Acknowledgment acknowledgment = ack;
			if (nacked.compareAndSet(false, true)) {
				// the listener sees [r1, r2]: r1 is processed, r2 is redelivered
				assertThat(records).containsExactly(r1, r2);
				acknowledgment.nack(1, Duration.ofMillis(50));
			}
			else {
				acknowledgment.acknowledge();
			}
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		// the interceptor drops the first record of the poll
		container.setBatchInterceptor((records, cons) -> new ConsumerRecords<>(Map.of(TP0, List.of(r1, r2)), Map.of()));
		container.start();
		try {
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(seeks).isNotEmpty());
			assertThat(seeks).containsExactly(2L);
		}
		finally {
			container.stop();
		}
	}

}
