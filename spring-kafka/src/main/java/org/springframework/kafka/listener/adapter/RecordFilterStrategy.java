/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Implementations of this interface can signal that a record about
 * to be delivered to a message listener should be discarded instead
 * of being delivered.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public interface RecordFilterStrategy<K, V> {

	/**
	 * Return true if the record should be discarded.
	 * @param consumerRecord the record.
	 * @return true to discard.
	 */
	boolean filter(ConsumerRecord<K, V> consumerRecord);

}
