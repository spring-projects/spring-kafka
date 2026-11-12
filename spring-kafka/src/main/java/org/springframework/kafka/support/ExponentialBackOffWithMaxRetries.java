/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.support;

import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Subclass of {@link ExponentialBackOff} that allows the specification of the maximum
 * number of retries rather than the maximum elapsed time.
 * <p>With a {@link #setJitter(long) jitter}, the intervals vary, so the number of retries
 * is only limited by {@link #getMaxAttempts()}, and the maximum elapsed time is not limited.
 *
 * @author Gary Russell
 * @author Jialin Chen
 * @author Yingzi Zhang
 * @since 2.7.3
 *
 */
public class ExponentialBackOffWithMaxRetries extends ExponentialBackOff {

	private final int maxRetries;

	/**
	 * Construct an instance that will calculate the {@link #setMaxElapsedTime(long)} from
	 * the maxRetries.
	 * @param maxRetries the max retries; 0 or less for no retries.
	 */
	@SuppressWarnings("this-escape")
	public ExponentialBackOffWithMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
		super.setMaxAttempts(Math.max(maxRetries, 0));
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
	public void setJitter(long jitter) {
		super.setJitter(jitter);
		calculateMaxElapsed();
	}

	@Override
	public void setMaxElapsedTime(long maxElapsedTime) {
		throw new IllegalStateException("'maxElapsedTime' is calculated from the 'maxRetries' property");
	}

	@SuppressWarnings("this-escape")
	private void calculateMaxElapsed() {
		if (this.maxRetries <= 0) {
			super.setMaxElapsedTime(0);
			return;
		}
		if (getJitter() > 0) {
			// The intervals vary, so the elapsed time can't tell when maxRetries is reached.
			super.setMaxElapsedTime(DEFAULT_MAX_ELAPSED_TIME);
			return;
		}
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
