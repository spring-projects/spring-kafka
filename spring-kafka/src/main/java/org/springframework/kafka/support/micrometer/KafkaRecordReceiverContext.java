/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.kafka.support.micrometer;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import io.micrometer.observation.transport.ReceiverContext;

/**
 * {@link ReceiverContext} for {@link ConsumerRecord}s.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class KafkaRecordReceiverContext extends ReceiverContext<ConsumerRecord<?, ?>> {

	private final String listenerId;

	private final ConsumerRecord<?, ?> record;

	public KafkaRecordReceiverContext(ConsumerRecord<?, ?> record, String listenerId) {
		super((carrier, key) -> {
			Header header = carrier.headers().lastHeader(key);
			if (header == null) {
				return null;
			}
			return new String(header.value(), StandardCharsets.UTF_8);
		});
		setCarrier(record);
		this.record = record;
		this.listenerId = listenerId;
	}

	public String getListenerId() {
		return this.listenerId;
	}

	/**
	 * Return the source topic.
	 * @return the source.
	 */
	public String getSource() {
		return this.record.topic();
	}

}
