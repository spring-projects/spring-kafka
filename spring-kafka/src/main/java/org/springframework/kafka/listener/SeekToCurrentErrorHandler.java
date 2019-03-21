/*
 * Copyright 2017 the original author or authors.
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.KafkaException;

/**
 * An error handler that seeks to the current offset for each topic in the remaining
 * records. Used to rewind partitions after a message failure so that it can be
 * replayed.
 *
 * @author Gary Russell
 * @since 2.0.1
 *
 */
public class SeekToCurrentErrorHandler implements ContainerAwareErrorHandler {

	@Override
	public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer, MessageListenerContainer container) {
		Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
		records.forEach(r ->
			offsets.computeIfAbsent(new TopicPartition(r.topic(), r.partition()), k -> r.offset()));
		offsets.forEach(consumer::seek);
		throw new KafkaException("Seek to current after exception", thrownException);
	}

}
