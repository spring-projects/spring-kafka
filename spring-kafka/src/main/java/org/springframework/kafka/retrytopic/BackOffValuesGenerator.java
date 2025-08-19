/*
 * Copyright 2018-present the original author or authors.
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

import java.util.List;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

/**
 *
 * Generates the back off values from the provided maxAttempts value and
 * {@link BackOff}.
 *
 * @author Tomaz Fernandes
 * @author Artem Bilan
 * @author Borahm Lee
 *
 * @since 2.7
 *
 */
public class BackOffValuesGenerator {

	private static final BackOff DEFAULT_BACKOFF = new FixedBackOff(1000);

	private final int numberOfValuesToCreate;

	private final BackOff backOff;

	public BackOffValuesGenerator(int providedMaxAttempts, @Nullable BackOff providedBackOff) {
		this.numberOfValuesToCreate = getMaxAttempts(providedMaxAttempts) - 1;
		this.backOff = providedBackOff != null ? providedBackOff : DEFAULT_BACKOFF;
	}

	private static int getMaxAttempts(int providedMaxAttempts) {
		return providedMaxAttempts != RetryTopicConstants.NOT_SET
				? providedMaxAttempts
				: RetryTopicConstants.DEFAULT_MAX_ATTEMPTS;
	}

	public List<Long> generateValues() {
		BackOffExecution backOffExecution = this.backOff.start();
		return Stream.generate(backOffExecution::nextBackOff).
				limit(this.numberOfValuesToCreate).toList();
	}

}
