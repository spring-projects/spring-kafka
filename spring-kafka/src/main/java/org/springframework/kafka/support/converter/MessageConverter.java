/*
 * Copyright 2016-2022 the original author or authors.
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

package org.springframework.kafka.support.converter;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.lang.Nullable;

/**
 * A top level interface for message converters.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
public interface MessageConverter {

	/**
	 * Get the thread bound group id.
	 * @return the group id.
	 */
	@Nullable
	static String getGroupId() {
		return KafkaUtils.getConsumerGroupId();
	}

	/**
	 * Set up the common headers.
	 * @param acknowledgment the acknowledgment.
	 * @param consumer the consumer.
	 * @param rawHeaders the raw headers map.
	 * @param theKey the key.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param offset the offfset.
	 * @param timestampType the timestamp type.
	 * @param timestamp the timestamp.
	 */
	default void commonHeaders(Acknowledgment acknowledgment, Consumer<?, ?> consumer, Map<String, Object> rawHeaders,
			@Nullable Object theKey, Object topic, Object partition, Object offset,
			@Nullable Object timestampType, Object timestamp) {

		rawHeaders.put(KafkaHeaders.RECEIVED_TOPIC, topic);
		rawHeaders.put(KafkaHeaders.RECEIVED_PARTITION, partition);
		rawHeaders.put(KafkaHeaders.OFFSET, offset);
		rawHeaders.put(KafkaHeaders.TIMESTAMP_TYPE, timestampType);
		rawHeaders.put(KafkaHeaders.RECEIVED_TIMESTAMP, timestamp);
		JavaUtils.INSTANCE
			.acceptIfNotNull(KafkaHeaders.RECEIVED_KEY, theKey, (key, val) -> rawHeaders.put(key, val))
			.acceptIfNotNull(KafkaHeaders.GROUP_ID, MessageConverter.getGroupId(),
					(key, val) -> rawHeaders.put(key, val))
			.acceptIfNotNull(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment, (key, val) -> rawHeaders.put(key, val))
			.acceptIfNotNull(KafkaHeaders.CONSUMER, consumer, (key, val) -> rawHeaders.put(key, val));
	}

}
