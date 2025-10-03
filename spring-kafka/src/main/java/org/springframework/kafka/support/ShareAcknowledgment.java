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

/**
 * A handle for acknowledging the delivery of a record when using share groups.
 * <p>
 * Share groups enable cooperative consumption where multiple consumers can process
 * records from the same partitions. Each record must be explicitly acknowledged
 * to indicate the result of processing.
 * <p>
 * Acknowledgment types:
 * <ul>
 * <li>{@code ACCEPT} - Record processed successfully</li>
 * <li>{@code RELEASE} - Temporary failure, make available for retry</li>
 * <li>{@code REJECT} - Permanent failure, do not retry</li>
 * </ul>
 * <p>
 * This interface is only applicable when using explicit acknowledgment mode
 * ({@code share.acknowledgement.mode=explicit}). In implicit mode, records are
 * automatically acknowledged as {@code ACCEPT}.
 * <p>
 * Note: Acknowledgment is separate from commit operations. After acknowledging
 * records, use {@code commitSync()} or {@code commitAsync()} to persist the
 * acknowledgments to the broker.
 *
 * @author Soby Chacko
 * @since 4.0
 */
public interface ShareAcknowledgment {

	/**
	 * Acknowledge the record as successfully processed.
	 * <p>
	 * The record will be marked as completed and will not be redelivered.
	 * The acknowledgment will be committed when:
	 * <ul>
	 * <li>The next {@code poll()} is called (batched with fetch)</li>
	 * <li>{@code commitSync()} or {@code commitAsync()} is explicitly called</li>
	 * <li>The consumer is closed</li>
	 * </ul>
	 *
	 * @throws IllegalStateException if the record has already been acknowledged
	 * @since 4.0
	 */
	void acknowledge();

	/**
	 * Release the record for redelivery due to a transient failure.
	 * <p>
	 * The record will be made available for another delivery attempt.
	 * The acknowledgment will be committed when:
	 * <ul>
	 * <li>The next {@code poll()} is called (batched with fetch)</li>
	 * <li>{@code commitSync()} or {@code commitAsync()} is explicitly called</li>
	 * <li>The consumer is closed</li>
	 * </ul>
	 *
	 * @throws IllegalStateException if the record has already been acknowledged
	 */
	void release();

	/**
	 * Reject the record due to a permanent failure.
	 * <p>
	 * The record will not be delivered again and will be archived.
	 * The acknowledgment will be committed when:
	 * <ul>
	 * <li>The next {@code poll()} is called (batched with fetch)</li>
	 * <li>{@code commitSync()} or {@code commitAsync()} is explicitly called</li>
	 * <li>The consumer is closed</li>
	 * </ul>
	 *
	 * @throws IllegalStateException if the record has already been acknowledged
	 */
	void reject();

}
