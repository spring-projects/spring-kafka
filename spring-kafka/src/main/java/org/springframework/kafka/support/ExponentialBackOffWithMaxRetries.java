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

import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Subclass of {@link ExponentialBackOff} that allows the specification of the maximum
 * number of retries rather than the maximum elapsed time.
 *
 * @author Gary Russell
 * @since 2.7.3
 *
 */
public class ExponentialBackOffWithMaxRetries extends ExponentialBackOff {

	private final int maxRetries;

	/**
	 * Construct an instance that will calculate the {@link #setMaxElapsedTime(long)} from
	 * the maxRetries.
	 * @param maxRetries the max retries.
	 */
	public ExponentialBackOffWithMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
		calculateMaxElapsed();
	}

	/**
	 * Get the max retries.
	 * @return the max retries.
	 */
	public int getMaxRetries() {
		return this.maxRetries;
	}

	@Override
	public void setInitialInterval(long initialInterval) {
		super.setInitialInterval(initialInterval);
		calculateMaxElapsed();
	}

	@Override
	public void setMultiplier(double multiplier) {
		super.setMultiplier(multiplier);
		calculateMaxElapsed();
	}

	@Override
	public void setMaxInterval(long maxInterval) {
		super.setMaxInterval(maxInterval);
		calculateMaxElapsed();
	}

	@Override
	public void setMaxElapsedTime(long maxElapsedTime) {
		throw new IllegalStateException("'maxElapsedTime' is calculated from the 'maxRetries' property");
	}

	private void calculateMaxElapsed() {
		long maxInterval = getMaxInterval();
		long maxElapsed = Math.min(getInitialInterval(), maxInterval);
		long current = maxElapsed;
		for (int i = 1; i < this.maxRetries; i++) {
			long next = Math.min((long) (current * getMultiplier()), maxInterval);
			current = next;
			maxElapsed += current;
		}
		super.setMaxElapsedTime(maxElapsed);
	}

}
