/*
 * Copyright 2016-2019 the original author or authors.
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

	@Nullable
	static String getGroupid() {
		String groupId = KafkaUtils.getConsumerGroupId();
		return groupId == null ? null : groupId;
	}

	default void commonHeaders(Acknowledgment acknowledgment, Consumer<?, ?> consumer, Map<String, Object> rawHeaders,
			Object theKey, Object topic, Object partition, Object offset, Object timestampType, Object timestamp) {

		rawHeaders.put(KafkaHeaders.RECEIVED_MESSAGE_KEY, theKey);
		rawHeaders.put(KafkaHeaders.RECEIVED_TOPIC, topic);
		rawHeaders.put(KafkaHeaders.RECEIVED_PARTITION_ID, partition);
		rawHeaders.put(KafkaHeaders.OFFSET, offset);
		rawHeaders.put(KafkaHeaders.TIMESTAMP_TYPE, timestampType);
		rawHeaders.put(KafkaHeaders.RECEIVED_TIMESTAMP, timestamp);
		JavaUtils.INSTANCE
			.acceptIfNotNull(KafkaHeaders.GROUP_ID, MessageConverter.getGroupid(),
					(key, val) -> rawHeaders.put(key, val))
			.acceptIfNotNull(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment, (key, val) -> rawHeaders.put(key, val))
			.acceptIfNotNull(KafkaHeaders.CONSUMER, consumer, (key, val) -> rawHeaders.put(key, val));
	}

}
