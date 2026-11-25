/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.support;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
 * @author Mohit Gupta
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

	@Test
	@Timeout(1)
	void largeMaxRetriesIsFast() {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(Integer.MAX_VALUE - 1);
		backOff.setInitialInterval(1_000L);
		backOff.setMaxInterval(10_000L);
		backOff.setMultiplier(2.0);
		assertThat(backOff.getMaxElapsedTime()).isEqualTo(21_474_836_435_000L);
	}

	@Test
	@Timeout(1)
	void largeMaxRetriesWithMultiplierOneIsFast() {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(Integer.MAX_VALUE);
		backOff.setInitialInterval(1_000L);
		backOff.setMaxInterval(10_000L);
		backOff.setMultiplier(1.0);
		assertThat(backOff.getMaxElapsedTime()).isEqualTo(Integer.MAX_VALUE * 1_000L);
	}

	@Test
	@Timeout(1)
	void largeMaxRetriesWithZeroInitialIntervalIsFast() {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(Integer.MAX_VALUE - 1);
		backOff.setInitialInterval(0L);
		backOff.setMaxInterval(10_000L);
		backOff.setMultiplier(2.0);
		assertThat(backOff.getMaxElapsedTime()).isZero();
	}

	@Test
	@Timeout(1)
	void largeMaxRetriesWithInitialGreaterThanMaxIntervalIsFast() {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(Integer.MAX_VALUE - 1);
		backOff.setInitialInterval(20_000L);
		backOff.setMaxInterval(10_000L);
		backOff.setMultiplier(2.0);
		assertThat(backOff.getMaxElapsedTime()).isEqualTo((Integer.MAX_VALUE - 1L) * 10_000L);
	}

	@ParameterizedTest
	@CsvSource({
			"1, 1000, 2.0, 10000",
			"5, 1000, 2.0, 10000",
			"10, 1000, 1.5, 30000",
			"20, 10, 1.05, 10000",
			"20, 1000, 1.0, 10000",
			"15, 20000, 2.0, 10000",
			"100, 100000000000000000, 2.0, 9223372036854775807"
	})
	void earlyTerminationMatchesNaiveLoop(int maxRetries, long initialInterval, double multiplier, long maxInterval) {
		ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
		backOff.setInitialInterval(initialInterval);
		backOff.setMultiplier(multiplier);
		backOff.setMaxInterval(maxInterval);

		long expected = Math.min(initialInterval, maxInterval);
		long current = expected;
		for (int i = 1; i < maxRetries; i++) {
			current = Math.min((long) (current * multiplier), maxInterval);
			expected += current;
		}
		assertThat(backOff.getMaxElapsedTime()).isEqualTo(expected);
	}

}
