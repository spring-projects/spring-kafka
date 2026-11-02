/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Michal Domagala
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

}
