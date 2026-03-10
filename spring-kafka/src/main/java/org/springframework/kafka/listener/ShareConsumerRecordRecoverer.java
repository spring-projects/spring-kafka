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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A strategy interface for determining the acknowledgment action when a share
 * consumer record fails processing. Implementations decide whether to
 * {@link AcknowledgeType#ACCEPT ACCEPT}, {@link AcknowledgeType#RELEASE RELEASE},
 * or {@link AcknowledgeType#REJECT REJECT} the failed record.
 *
 * <p>The default implementation ({@link DefaultShareConsumerRecordRecoverer}) REJECTs
 * all exceptions. Users can provide custom implementations to RELEASE records for
 * transient exceptions (e.g., downstream service timeouts), allowing the broker to
 * redeliver them to another consumer.
 *
 * @author Soby Chacko
 * @since 4.1
 * @see DefaultShareConsumerRecordRecoverer
 * @see ShareKafkaMessageListenerContainer
 */
@FunctionalInterface
public interface ShareConsumerRecordRecoverer {

	/**
	 * Determine the acknowledgment action for a failed record.
	 * @param record the record that failed processing
	 * @param exception the exception thrown during processing
	 * @return the {@link AcknowledgeType} to use — typically REJECT or RELEASE
	 */
	AcknowledgeType recover(ConsumerRecord<?, ?> record, Exception exception);

}
