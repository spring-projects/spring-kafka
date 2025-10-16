/*
 * Copyright 2024-present the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * An interface to indicate that a message listener adapter can report
 * whether a record was filtered during processing.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Chaedong Im
 * @since 4.0
 */
public interface FilteringAware<K, V> {

	/**
	 * Check if the most recent record processed was filtered out.
	 * This method should be called after a record has been processed
	 * to determine if the record was filtered and should not trigger
	 * an offset commit in RECORD_FILTERED acknowledge mode.
	 * @param record the record to check
	 * @return true if the record was filtered, false if it was processed
	 */
	boolean wasFiltered(ConsumerRecord<K, V> record);

}
