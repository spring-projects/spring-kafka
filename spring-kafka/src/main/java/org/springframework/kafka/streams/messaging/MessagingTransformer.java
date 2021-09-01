/*
 * Copyright 2019-2021 the original author or authors.
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

package org.springframework.kafka.streams.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * A {@link Transformer} implementation that invokes a {@link MessagingFunction}
 * converting to/from spring-messaging {@link Message}. Can be used, for example,
 * to invoke a Spring Integration flow.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <R> the result value type.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class MessagingTransformer<K, V, R> implements Transformer<K, V, KeyValue<K, R>> {

	private final MessagingFunction function;

	private final MessagingMessageConverter converter;

	private ProcessorContext processorContext;

	/**
	 * Construct an instance with the provided function and converter.
	 * @param function the function.
	 * @param converter the converter.
	 */
	public MessagingTransformer(MessagingFunction function, MessagingMessageConverter converter) {
		Assert.notNull(function, "'function' cannot be null");
		Assert.notNull(converter, "'converter' cannot be null");
		this.function = function;
		this.converter = converter;
	}

	@Override
	public void init(ProcessorContext context) {
		this.processorContext = context;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KeyValue<K, R> transform(K key, V value) {
		Headers headers = this.processorContext.headers();
		ConsumerRecord<Object, Object> record = new ConsumerRecord<Object, Object>(this.processorContext.topic(),
				this.processorContext.partition(), this.processorContext.offset(),
				this.processorContext.timestamp(), TimestampType.NO_TIMESTAMP_TYPE,
				0, 0,
				key, value,
				headers, Optional.empty());
		Message<?> message = this.converter.toMessage(record, null, null, null);
		message = this.function.exchange(message);
		List<String> headerList = new ArrayList<>();
		headers.forEach(header -> headerList.add(header.key()));
		headerList.forEach(name -> headers.remove(name));
		ProducerRecord<?, ?> fromMessage = this.converter.fromMessage(message, "dummy");
		fromMessage.headers().forEach(header -> {
			if (!header.key().equals(KafkaHeaders.TOPIC)) {
				headers.add(header);
			}
		});
		Object key2 = message.getHeaders().get(KafkaHeaders.MESSAGE_KEY);
		return new KeyValue(key2 == null ? key : key2, message.getPayload());
	}

	@Override
	public void close() {
		// NO-OP
	}

}
