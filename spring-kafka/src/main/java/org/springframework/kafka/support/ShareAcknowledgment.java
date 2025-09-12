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

package org.springframework.kafka.support;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.jspecify.annotations.Nullable;

/**
 * A handle for acknowledging the delivery of a record when using share groups.
 * <p>
 * Share groups enable cooperative consumption where multiple consumers can process
 * records from the same partitions. Each record must be explicitly acknowledged
 * to indicate the result of processing.
 * <p>
 * Acknowledgment types:
 * <ul>
 * <li>{@link AcknowledgeType#ACCEPT} - Record processed successfully</li>
 * <li>{@link AcknowledgeType#RELEASE} - Temporary failure, make available for retry</li>
 * <li>{@link AcknowledgeType#REJECT} - Permanent failure, do not retry</li>
 * </ul>
 * <p>
 * This interface is only applicable when using explicit acknowledgment mode
 * ({@code share.acknowledgement.mode=explicit}). In implicit mode, records are
 * automatically acknowledged as {@link AcknowledgeType#ACCEPT}.
 * <p>
 * Note: Acknowledgment is separate from commit operations. After acknowledging
 * records, use {@code commitSync()} or {@code commitAsync()} to persist the
 * acknowledgments to the broker.
 *
 * @author Soby Chacko
 * @since 4.0
 * @see AcknowledgeType
 */
public interface ShareAcknowledgment {

	/**
	 * Acknowledge the delivery of the record with the specified type.
	 * <p>
	 * The acknowledgment will be committed when:
	 * <ul>
	 * <li>The next {@code poll()} is called (batched with fetch)</li>
	 * <li>{@code commitSync()} or {@code commitAsync()} is explicitly called</li>
	 * <li>The consumer is closed</li>
	 * </ul>
	 *
	 * @param type the acknowledgment type indicating the result of processing
	 * @throws IllegalStateException if the record has already been acknowledged
	 * @throws IllegalArgumentException if the acknowledgment type is null
	 */
	void acknowledge(AcknowledgeType type);

	/**
	 * Acknowledge the record as successfully processed.
	 * <p>
	 * This is equivalent to {@code acknowledge(AcknowledgeType.ACCEPT)}.
	 * The record will be marked as completed and will not be redelivered.
	 *
	 * @throws IllegalStateException if the record has already been acknowledged
	 * @since 4.0
	 */
	default void acknowledge() {
		acknowledge(AcknowledgeType.ACCEPT);
	}

	/**
	 * Release the record for redelivery due to a transient failure.
	 * <p>
	 * This is a convenience method equivalent to {@code acknowledge(AcknowledgeType.RELEASE)}.
	 * The record will be made available for another delivery attempt.
	 */
	default void release() {
		acknowledge(AcknowledgeType.RELEASE);
	}

	/**
	 * Reject the record due to a permanent failure.
	 * <p>
	 * This is a convenience method equivalent to {@code acknowledge(AcknowledgeType.REJECT)}.
	 * The record will not be delivered again and will be archived.
	 */
	default void reject() {
		acknowledge(AcknowledgeType.REJECT);
	}

	/**
	 * Check if this record has already been acknowledged.
	 *
	 * @return true if the record has been acknowledged, false otherwise
	 */
	boolean isAcknowledged();

	/**
	 * Get the acknowledgment type that was used to acknowledge this record.
	 *
	 * @return the acknowledgment type, or null if not yet acknowledged
	 */
	@Nullable
	AcknowledgeType getAcknowledgmentType();

}
