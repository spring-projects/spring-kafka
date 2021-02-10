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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.lang.Nullable;

/**
 * A {@link ConsumerRecordRecoverer} that supports getting a reference to the
 * {@link Consumer}.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
@FunctionalInterface
public interface ConsumerAwareRecordRecoverer extends ConsumerRecordRecoverer {


	@Override
	default void accept(ConsumerRecord<?, ?> record, Exception exception) {
		accept(record, null, exception);
	}

	/**
	 * Recover the record.
	 * @param record the record.
	 * @param consumer the consumer.
	 * @param exception the exception.
	 * @since 2.7
	 */
	void accept(ConsumerRecord<?, ?> record, @Nullable Consumer<?, ?> consumer, Exception exception);

}
