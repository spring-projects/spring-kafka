/*
 * Copyright 2021 the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.lang.Nullable;

/**
 * Called to determine whether a record should be skipped.
 *
 * @author Gary Russell
 * @since 2.7
 */
@FunctionalInterface
public interface RecoveryStrategy {

	/**
	 * Return true if the record should be skipped because it was successfully
	 * recovered.
	 * @param record the record.
	 * @param ex the exception.
	 * @param container the container (or parent if a child container).
	 * @return true to skip.
	 * @throws InterruptedException if the thread is interrupted.
	 */
	boolean recovered(ConsumerRecord<?, ?> record, Exception ex, @Nullable MessageListenerContainer container)
			throws InterruptedException;

}
