/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.kafka.requestreply;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * This is a {@link ReplyingKafkaTemplate} that adds a simple validation semantic so it can take multiple responses and
 * choose the first one that matches the validation condition.
 * <p>
 * The use case for this would be providing a farm of topic responders which are decoupled from the calling service and
 * the calling service does not know who would respond and when, but knows some property of the response to consider it
 * <em>valid</em>.
 * <p>
 * This can be explained using a dinner party analogy:
 * <ol>
 * <li>0:00 Kid: Does anyone know what the answers are to the square root of 144 and 2+2?
 * <li>0:01 Uncle 1: 13, 5
 * <li>0:02 Uncle 2: 12, 4
 * <li>0:05 Kid: okay I gathered a few answers,
 * <li>0:05 Kid: Filter out who can't answer 2+2
 * <li>0:05 Kid: The proper answer is 12, 4
 * <li>0:06 Uncle 3: 12, 4
 * <li>0:06 Kid: Sorry uncle 3 you're too slow, so I am ignoring you
 * </ol>
 *
 * @param <K> key
 * @param <V> request value
 * @param <R> response value
 *
 * @author Archimedes Trajano
 */
public class ValidatingReplyingKafkaTemplate<K, V, R> extends ReplyingKafkaTemplate<K, V, R> {

	/**
	 * Validation predicate.
	 */
	private final BiPredicate<K, R> validationPredicate;

	public ValidatingReplyingKafkaTemplate(
		ProducerFactory<K, V> producerFactory,
		GenericMessageListenerContainer<K, R> replyContainer,
		BiPredicate<K, R> validationPredicate
	) {
		super(producerFactory, replyContainer);
		this.validationPredicate = validationPredicate;
	}

	public ValidatingReplyingKafkaTemplate(
		ProducerFactory<K, V> producerFactory,
		GenericMessageListenerContainer<K, R> replyContainer,
		boolean autoFlush,
		BiPredicate<K, R> validationPredicate
	) {
		super(producerFactory, replyContainer, autoFlush);
		this.validationPredicate = validationPredicate;
	}

	/**
	 * Filter out records that do not pass the validation predicate.
	 * <p>
	 * This does an initial filter to make sure only the ones with a correlation ID defined is processed.  This does
	 * <b>not</b> check whether the correlation ID is something that needs to be considered as {@code futures} is not
	 * accessible and it is relying on the super class to perform the extra test.
	 */
	@Override
	public void onMessage(List<ConsumerRecord<K, R>> data) {
		super.onMessage(data.stream()
			.filter(record -> record.headers().lastHeader(KafkaHeaders.CORRELATION_ID) != null)
			.filter(record -> this.validationPredicate.test(record.key(), record.value()))
			.collect(Collectors.toList()));
	}

}
