/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.NoBackOffPolicy;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
class BackOffValuesGeneratorTests {

	@Test
	void shouldGenerateWithDefaultValues() {
		// Default MAX_ATTEMPTS = 3
		// Default Policy = FixedBackoffPolicy

		// setup
		BackOffValuesGenerator backOffValuesGenerator = new BackOffValuesGenerator(-1, null);

		// when
		List<Long> backOffValues = backOffValuesGenerator.generateValues();

		// then
		List<Long> expectedBackOffs = Arrays.asList(1000L, 1000L);
		assertThat(backOffValues).isEqualTo(expectedBackOffs);
	}

	@Test
	void shouldGenerateExponentialValues() {

		// setup
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setMultiplier(2);
		backOffPolicy.setInitialInterval(1000);
		BackOffValuesGenerator backOffValuesGenerator = new BackOffValuesGenerator(4, backOffPolicy);

		// when
		List<Long> backOffValues = backOffValuesGenerator.generateValues();

		// then
		List<Long> expectedBackoffs = Arrays.asList(1000L, 2000L, 4000L);
		assertThat(backOffValues).isEqualTo(expectedBackoffs);
	}

	@Test
	void shouldGenerateWithNoBackOff() {

		// setup
		BackOffPolicy backOffPolicy = new NoBackOffPolicy();
		BackOffValuesGenerator backOffValuesGenerator = new BackOffValuesGenerator(4, backOffPolicy);

		// when
		List<Long> backOffValues = backOffValuesGenerator.generateValues();

		// then
		List<Long> expectedBackoffs = Arrays.asList(0L, 0L, 0L);
		assertThat(backOffValues).isEqualTo(expectedBackoffs);
	}
}
