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

package org.springframework.kafka.listener.adapter;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConsumerAwareMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.Assert;

/**
 * A {@link AcknowledgingConsumerAwareMessageListener} adapter that implements
 * converting received {@link ConsumerRecord} using specified {@link MessageConverter}
 * and then passes result to specified {@link MessageListener}.
 *
 * @param <V> the desired value type after conversion.
 *
 * @author Adrian Chlebosz
 * @since 3.0
 * @see AcknowledgingConsumerAwareMessageListener
 */
@SuppressWarnings("rawtypes")
public class ConvertingMessageListener<V> implements AcknowledgingConsumerAwareMessageListener<Object, Object> {

	private final MessageListener delegate;
	private final Class<V> desiredValueType;

	private MessageConverter messageConverter;

	/**
	 * Construct an instance with the provided {@link MessageListener} and {@link Class}
	 * as a desired type of {@link ConsumerRecord}'s value after conversion. Default value of
	 * {@link MessageConverter} is used, which is {@link GenericMessageConverter}.
	 *
	 * @param delegateMessageListener the {@link MessageListener} to use when passing converted {@link ConsumerRecord} further.
	 * @param desiredValueType the {@link Class} setting desired type of {@link ConsumerRecord}'s value.
	 */
	public ConvertingMessageListener(MessageListener<?, V> delegateMessageListener, Class<V> desiredValueType) {
		Assert.notNull(delegateMessageListener, "'delegateMessageListener' cannot be null");
		Assert.notNull(desiredValueType, "'desiredValueType' cannot be null");
		this.delegate = delegateMessageListener;
		this.desiredValueType = desiredValueType;

		this.messageConverter = new GenericMessageConverter();
	}

	/**
	 * Set a {@link MessageConverter}.
	 * @param messageConverter the message converter to use for conversion of incoming {@link ConsumerRecord}.
	 * @since 3.0
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' cannot be null");
		this.messageConverter = messageConverter;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onMessage(ConsumerRecord data, Acknowledgment acknowledgment, Consumer consumer) {
		ConsumerRecord convertedConsumerRecord = convertConsumerRecord(data);
		if (this.delegate instanceof AcknowledgingConsumerAwareMessageListener) {
			this.delegate.onMessage(convertedConsumerRecord, acknowledgment, consumer);
		}
		else if (this.delegate instanceof ConsumerAwareMessageListener) {
			this.delegate.onMessage(convertedConsumerRecord, consumer);
		}
		else if (this.delegate instanceof AcknowledgingMessageListener) {
			this.delegate.onMessage(convertedConsumerRecord, acknowledgment);
		}

		this.delegate.onMessage(convertedConsumerRecord);
	}

	private ConsumerRecord convertConsumerRecord(ConsumerRecord data) {
		Header[] headerArray = data.headers().toArray();
		Map<String, Object> headerMap = Arrays.stream(headerArray)
				.collect(Collectors.toMap(Header::key, Header::value));

		Message message = new GenericMessage<>(data.value(), headerMap);
		Object converted = this.messageConverter.fromMessage(message, this.desiredValueType);

		if (converted == null) {
			throw new MessageConversionException(message, "Message cannot be converted by used MessageConverter");
		}

		return rebuildConsumerRecord(data, converted);
	}

	private static ConsumerRecord rebuildConsumerRecord(ConsumerRecord data, Object converted) {
		return new ConsumerRecord<>(
				data.topic(),
				data.partition(),
				data.offset(),
				data.key(),
				converted
		);
	}

}
