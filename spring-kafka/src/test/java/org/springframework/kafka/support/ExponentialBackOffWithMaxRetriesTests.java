/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.support;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * @author Gary Russell
 * @author Jialin Chen
 * @author Yingzi Zhang
 * @since 2.7.3
 *
 */
public class ExponentialBackOffWithMaxRetriesTests {

	@ParameterizedTest
	@ValueSource(ints = {0, -1})
	void nonPositiveRetriesStopImmediately(int maxRetries) {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);

		assertThat(backOff.start().nextBackOff()).isEqualTo(BackOffExecution.STOP);
	}

	@ParameterizedTest
	@ValueSource(ints = {0, -1})
	void nonPositiveRetriesStopAfterIntervalConfiguration(int maxRetries) {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
		backOff.setInitialInterval(100L);
		assertThat(backOff.start().nextBackOff()).isEqualTo(BackOffExecution.STOP);

		backOff.setMultiplier(2.0);
		assertThat(backOff.start().nextBackOff()).isEqualTo(BackOffExecution.STOP);

		backOff.setMaxInterval(1_000L);
		assertThat(backOff.start().nextBackOff()).isEqualTo(BackOffExecution.STOP);
	}

	@ParameterizedTest
	@CsvSource({
			"3, 1000, 2.0, 30000, 500",
			"5, 1000, 1.5, 30000, 800",
			"5, 1000, 2.0, 4000, 1000"
	})
	void jitterKeepsTheNumberOfRetries(int maxRetries, long initialInterval, double multiplier, long maxInterval,
			long jitter) {

		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
		backOff.setInitialInterval(initialInterval);
		backOff.setMultiplier(multiplier);
		backOff.setMaxInterval(maxInterval);
		backOff.setJitter(jitter);

		for (int i = 0; i < 1000; i++) {
			BackOffExecution execution = backOff.start();
			for (int retry = 0; retry < maxRetries; retry++) {
				assertThat(execution.nextBackOff()).isNotEqualTo(BackOffExecution.STOP);
			}
			assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
		}
		assertThat(backOff.getMaxElapsedTime()).isEqualTo(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME);
	}

	@Test
	void oneRetryIsPreserved() {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(1);
		BackOffExecution execution = backOff.start();

		assertThat(execution.nextBackOff()).isEqualTo(2_000L);
		assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
	}

	@Test
	void calcAll() {
		ExponentialBackOffWithMaxRetries bo = new ExponentialBackOffWithMaxRetries(10);
		bo.setInitialInterval(1_000L);
		bo.setMultiplier(2.0);
		bo.setMaxInterval(10_000L);
		assertThatIllegalStateException().isThrownBy(() -> bo.setMaxElapsedTime(42L));
		assertThat(bo.getMaxRetries()).isEqualTo(10);
		List<Long> delays = new ArrayList<>();
		BackOffExecution boEx = bo.start();
		IntStream.range(0, 11).forEach(i -> delays.add(boEx.nextBackOff()));
		assertThat(delays).containsExactly(1_000L, 2_000L, 4_000L, 8_000L, 10_000L, 10_000L, 10_000L, 10_000L, 10_000L,
				10_000L, -1L);
	}

	@Test
	void calcMaxLessThanInitial() {
		ExponentialBackOffWithMaxRetries bo = new ExponentialBackOffWithMaxRetries(3);
		bo.setInitialInterval(10_000L);
		bo.setMultiplier(2.0);
		bo.setMaxInterval(5_000L);
		List<Long> delays = new ArrayList<>();
		BackOffExecution boEx = bo.start();
		IntStream.range(0, 4).forEach(i -> delays.add(boEx.nextBackOff()));
		assertThat(delays).containsExactly(5_000L, 5_000L, 5_000L, -1L);
	}

	@Test
	void calcDefault() {
		ExponentialBackOffWithMaxRetries bo = new ExponentialBackOffWithMaxRetries(10);
		List<Long> delays = new ArrayList<>();
		BackOffExecution boEx = bo.start();
		IntStream.range(0, 11).forEach(i -> delays.add(boEx.nextBackOff()));
		assertThat(delays).containsExactly(2_000L, 3_000L, 4_500L, 6_750L, 10_125L, 15_187L, 22_780L, 30_000L, 30_000L,
				30_000L, -1L);
	}

}
