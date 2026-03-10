/*
 * Copyright 2025-present the original author or authors.
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

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.core.log.LogAccessor;

/**
 * Default {@link ShareConsumerRecordRecoverer} that REJECTs all failed records.
 *
 * <p>This matches the behavior of the share consumer container prior to the
 * introduction of pluggable error handling. All exceptions — whether transient
 * or permanent — result in the record being rejected (archived, no redelivery).
 *
 * <p>Users who want to RELEASE records for specific transient exception types
 * can provide a custom implementation:
 *
 * <pre>{@code
 * factory.setShareConsumerRecordRecoverer((record, ex) -> {
 *     if (ex instanceof TransientException) {
 *         return AcknowledgeType.RELEASE;
 *     }
 *     return AcknowledgeType.REJECT;
 * });
 * }</pre>
 *
 * @author Soby Chacko
 * @since 4.1
 * @see ShareConsumerRecordRecoverer
 */
public class DefaultShareConsumerRecordRecoverer implements ShareConsumerRecordRecoverer {

	private static final LogAccessor logger = new LogAccessor(
			LogFactory.getLog(DefaultShareConsumerRecordRecoverer.class));

	@Override
	public AcknowledgeType recover(ConsumerRecord<?, ?> record, Exception exception) {
		logger.error(exception, () -> "Share consumer record processing failed; rejecting record from "
				+ record.topic() + "-" + record.partition() + "@" + record.offset());
		return AcknowledgeType.REJECT;
	}

}
