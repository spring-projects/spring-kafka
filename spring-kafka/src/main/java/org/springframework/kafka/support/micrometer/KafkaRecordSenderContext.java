/*
 * Copyright 2022-present the original author or authors.
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
import java.util.function.Supplier;

import io.micrometer.observation.transport.SenderContext;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

/**
 * {@link SenderContext} for {@link ProducerRecord}s.
 *
 * @author Gary Russell
 * @author Christian Mergenthaler
 * @author Wang Zhiyang
 * @author Soby Chacko
 *
 * @since 3.0
 *
 */
public class KafkaRecordSenderContext extends SenderContext<ProducerRecord<?, ?>> {

	private final String beanName;

	private final ProducerRecord<?, ?> record;

	@SuppressWarnings("this-escape")
	public KafkaRecordSenderContext(ProducerRecord<?, ?> record, String beanName, Supplier<String> clusterId) {
		super((carrier, key, value) -> {
			Headers headers = record.headers();
			headers.remove(key);
			headers.add(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
		});

		setCarrier(record);
		this.beanName = beanName;
		this.record = record;
		String cluster = clusterId.get();
		setRemoteServiceName("Apache Kafka" + (cluster != null ? ": " + cluster : ""));
	}

	/**
	 * Return the template's bean name.
	 * @return the name.
	 */
	public String getBeanName() {
		return this.beanName;
	}

	/**
	 * Return the destination topic.
	 * @return the topic.
	 */
	public String getDestination() {
		return this.record.topic();
	}

	/**
	 * Return the producer record.
	 * @return the record.
	 * @since 3.0.6
	 */
	public ProducerRecord<?, ?> getRecord() {
		return this.record;
	}

}
