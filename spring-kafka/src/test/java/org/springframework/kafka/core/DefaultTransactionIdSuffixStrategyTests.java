/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.core;

import java.util.Map;
import java.util.Queue;

import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * @author Ilya Starchenko
 *
 * @since 3.2
 */
class DefaultTransactionIdSuffixStrategyTests {

	@Test
	void acquireSuffixWithoutCache() {
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(0);
		String txIdPrefix = "txIdPrefix";
		String suffix = suffixStrategy.acquireSuffix(txIdPrefix);
		assertThat(suffix).isNotNull();
	}

	@Test
	void acquireSuffixWithCache() {
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(1);
		String txIdPrefix = "txIdPrefix";
		String suffix = suffixStrategy.acquireSuffix(txIdPrefix);
		assertThat(suffix).isNotNull();
	}

	@Test
	void acquireSuffixWithCacheExhausted() {
		String txIdPrefix = "txIdPrefix";
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(2);
		String suffix1 = suffixStrategy.acquireSuffix(txIdPrefix);
		String suffix2 = suffixStrategy.acquireSuffix(txIdPrefix);
		assertThatExceptionOfType(NoProducerAvailableException.class)
				.isThrownBy(() -> suffixStrategy.acquireSuffix(txIdPrefix))
				.extracting(NoProducerAvailableException::getTxIdPrefix).isEqualTo(txIdPrefix);
	}

	@Test
	void releaseSuffixWithCache() {
		String txIdPrefix = "txIdPrefix";
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(2);
		String suffix = suffixStrategy.acquireSuffix(txIdPrefix);
		assertThatNoException().isThrownBy(() -> suffixStrategy.releaseSuffix(txIdPrefix, suffix));
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	void releaseAllSuffixesWithCache() {
		String txIdPrefix = "txIdPrefix";
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(2);
		String suffix1 = suffixStrategy.acquireSuffix(txIdPrefix);
		String suffix2 = suffixStrategy.acquireSuffix(txIdPrefix);
		assertThatNoException().isThrownBy(() -> suffixStrategy.releaseSuffix(txIdPrefix, suffix1));
		Map<?, ?> suffixCache = KafkaTestUtils.getPropertyValue(suffixStrategy, "suffixCache", Map.class);
		assertThat(suffixCache).hasSize(1);
		Queue queue = (Queue) suffixCache.get(txIdPrefix);
		assertThat(queue).hasSize(1);
		assertThat(queue.poll()).isEqualTo(suffix1);
	}

	@Test
	void setMaxCacheIsNegative() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new DefaultTransactionIdSuffixStrategy(-1));
	}

	@Test
	void setMaxCacheIsZero() {
		assertThatNoException().isThrownBy(() -> new DefaultTransactionIdSuffixStrategy(0));
	}
}
