/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.support;

import java.util.Objects;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.TopicPartitionOffset.SeekPosition;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.3.13
 *
 */
public class TopicPartitionOffsetTests {

	@Test
	void hashCodeTest() {
		assertThat(new TopicPartitionOffset("foo", 1, SeekPosition.BEGINNING).hashCode())
				.isNotEqualTo(new TopicPartitionOffset("foo",  1, SeekPosition.END).hashCode());
	}

	@Test
	void hashCodeNPE() {
		assertThat(new TopicPartitionOffset("foo", 0).hashCode())
				.isEqualTo(Objects.hash(new TopicPartition("foo", 0), null));
	}

}
