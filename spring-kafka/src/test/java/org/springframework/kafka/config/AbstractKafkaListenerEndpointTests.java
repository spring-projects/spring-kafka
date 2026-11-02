/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.kafka.config;

import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.TopicPartitionOffset;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * @author OhKyu Chan
 *
 * @since 4.0
 */
class AbstractKafkaListenerEndpointTests {

	@Test
	void setTopicsRejectsNullNamingItsOwnProperty() {
		MethodKafkaListenerEndpoint<String, String> endpoint = new MethodKafkaListenerEndpoint<>();

		assertThatIllegalArgumentException()
				.isThrownBy(() -> endpoint.setTopics((String[]) null))
				.withMessage("'topics' must not be null");
	}

	@Test
	void setTopicPartitionsRejectsNullNamingItsOwnProperty() {
		MethodKafkaListenerEndpoint<String, String> endpoint = new MethodKafkaListenerEndpoint<>();

		assertThatIllegalArgumentException()
				.isThrownBy(() -> endpoint.setTopicPartitions((TopicPartitionOffset[]) null))
				.withMessage("'topicPartitions' must not be null");
	}

}
