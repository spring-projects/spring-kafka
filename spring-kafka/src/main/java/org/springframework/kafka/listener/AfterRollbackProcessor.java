/*
 * Copyright 2018 the original author or authors.
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

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Invoked by a listener container with remaining, unprocessed, records
 * (including the failed record). Implementations should seek the desired
 * topics/partitions so that records will be re-fetched on the next
 * poll. When used with a batch listener, the entire batch of records is
 * provided.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 1.3.5
 *
 */
public interface AfterRollbackProcessor<K, V> {

	/**
	 * Process the remaining records.
	 * @param records the records.
	 * @param consumer the consumer.
	 */
	void process(List<ConsumerRecord<K, V>> records, Consumer<K, V> consumer);

}
