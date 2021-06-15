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

package org.springframework.kafka.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.springframework.util.backoff.BackOffExecution;

/**
 * @author Gary Russell
 * @since 2.7.3
 *
 */
public class ExponentialBackOffWithMaxRetriesTests {

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
