/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.core;

import org.apache.kafka.clients.producer.Producer;

import org.springframework.transaction.support.ResourceHolderSupport;

/**
 * Kafka resource holder, wrapping a Kafka producer. KafkaTransactionManager binds instances of this
 * class to the thread, for a given Kafka producer factory.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 */
public class KafkaResourceHolder<K, V> extends ResourceHolderSupport {

	private final Producer<K, V> producer;

	/**
	 * Construct an instance for the producer.
	 * @param producer the producer.
	 */
	public KafkaResourceHolder(Producer<K, V> producer) {
		this.producer = producer;
	}

	public Producer<K, V> getProducer() {
		return this.producer;
	}

	public void commit() {
		this.producer.commitTransaction();
	}

	public void close() {
		this.producer.close();
	}

	public void rollback() {
		this.producer.abortTransaction();
	}

}
