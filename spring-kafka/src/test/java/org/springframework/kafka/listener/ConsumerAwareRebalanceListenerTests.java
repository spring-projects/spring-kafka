/*
 * Copyright 2021 the original author or authors.
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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
 * @since 2.6.5
 *
 */
public class ConsumerAwareRebalanceListenerTests {

	@Test
	void nonConsumerAwareTestAssigned() {
		AtomicBoolean called = new AtomicBoolean();
		new ConsumerAwareRebalanceListener() {

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				called.set(true);
			}

		}.onPartitionsAssigned(null, null);
		assertThat(called.get()).isTrue();
	}

	@Test
	void nonConsumerAwareTestAssignedThrows() {
		AtomicBoolean called = new AtomicBoolean();
		new ConsumerAwareRebalanceListener() {

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				called.set(true);
				throw new RuntimeException();
			}

		}.onPartitionsAssigned(null, null);
		assertThat(called.get()).isTrue();
	}

	@Test
	void nonConsumerAwareTestRevoked() {
		AtomicBoolean called = new AtomicBoolean();
		new ConsumerAwareRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				called.set(true);
			}

		}.onPartitionsRevokedBeforeCommit(null, null);
		assertThat(called.get()).isTrue();
	}

	@Test
	void nonConsumerAwareTestRevokedThrows() {
		AtomicBoolean called = new AtomicBoolean();
		new ConsumerAwareRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				called.set(true);
				throw new RuntimeException();
			}

		}.onPartitionsRevokedBeforeCommit(null, null);
		assertThat(called.get()).isTrue();
	}

	@Test
	void nonConsumerAwareTestLost() {
		AtomicBoolean called = new AtomicBoolean();
		new ConsumerAwareRebalanceListener() {

			@Override
			public void onPartitionsLost(Collection<TopicPartition> partitions) {
				called.set(true);
			}

		}.onPartitionsLost(null, null);
		assertThat(called.get()).isTrue();
	}

	@Test
	void nonConsumerAwareTestLostThrows() {
		AtomicBoolean called = new AtomicBoolean();
		new ConsumerAwareRebalanceListener() {

			@Override
			public void onPartitionsLost(Collection<TopicPartition> partitions) {
				called.set(true);
				throw new RuntimeException();
			}

		}.onPartitionsLost(null, null);
		assertThat(called.get()).isTrue();
	}

}
