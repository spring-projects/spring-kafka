/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
