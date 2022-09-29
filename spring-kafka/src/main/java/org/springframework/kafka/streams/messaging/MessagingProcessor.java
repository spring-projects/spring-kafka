/*
 * Copyright 2019-2022 the original author or authors.
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
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * A {@link Transformer} implementation that invokes a {@link MessagingFunction}
 * converting to/from spring-messaging {@link Message}. Can be used, for example,
 * to invoke a Spring Integration flow.
 *
 * @param <Kin> the input key type.
 * @param <Vin> the input value type.
 * @param <Kout> the output key type.
 * @param <Vout> the output value type.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class MessagingProcessor<Kin, Vin, Kout, Vout> extends ContextualProcessor<Kin, Vin, Kout, Vout> {

	private final MessagingFunction function;

	private final MessagingMessageConverter converter;

	/**
	 * Construct an instance with the provided function and converter.
	 * @param function the function.
	 * @param converter the converter.
	 */
	public MessagingProcessor(MessagingFunction function, MessagingMessageConverter converter) {
		Assert.notNull(function, "'function' cannot be null");
		Assert.notNull(converter, "'converter' cannot be null");
		this.function = function;
		this.converter = converter;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void process(Record<Kin, Vin> record) {
		ProcessorContext<Kout, Vout> context = context();
		RecordMetadata meta = context.recordMetadata().orElse(null);
		Assert.state(meta != null, "No record metadata present");
		Headers headers = record.headers();
		ConsumerRecord<Object, Object> rebuilt = new ConsumerRecord<Object, Object>(meta.topic(),
				meta.partition(), meta.offset(),
				record.timestamp(), TimestampType.NO_TIMESTAMP_TYPE,
				0, 0,
				record.key(), record.value(),
				headers, Optional.empty());
		Message<?> message = this.converter.toMessage(rebuilt, null, null, null);
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
		context.forward(new Record<>((Kout) message.getHeaders().get(KafkaHeaders.KEY), (Vout) message.getPayload(),
				record.timestamp(), headers));
	}

	@Override
	public void close() {
		// NO-OP
	}

}
