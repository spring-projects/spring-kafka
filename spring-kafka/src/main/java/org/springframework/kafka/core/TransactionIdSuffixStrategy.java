/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.core;

import org.jspecify.annotations.Nullable;

/**
 * The strategy for managing transactional producer suffixes.
 *
 * @author Ilya Starchenko
 *
 * @since 3.2
 */
public interface TransactionIdSuffixStrategy {

	/**
	 * Acquire the suffix for the transactional producer.
	 * @param txIdPrefix the transaction id prefix.
	 * @return the suffix.
	 */
	String acquireSuffix(@Nullable String txIdPrefix);

	/**
	 * Release the suffix for the transactional producer.
	 * @param txIdPrefix the transaction id prefix.
	 * @param suffix the suffix.
	 */
	void releaseSuffix(@Nullable String txIdPrefix, @Nullable String suffix);

}
