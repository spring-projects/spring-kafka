/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.retrytopic;

import java.util.List;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 *
 * Generates the back off values from the provided maxAttempts value and
 * {@link BackOff}.
 *
 * @author Tomaz Fernandes
 * @author Artem Bilan
 * @author Borahm Lee
 * @author Yingzi Zhang
 * @author Tanishq Choudhary
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

	/**
	 * Generate the back off values, stopping early if the {@link BackOff} is exhausted
	 * before {@code maxAttempts} is reached.
	 * @return the back off values.
	 */
	public List<Long> generateValues() {
		return generateValues(this.backOff);
	}

	/**
	 * Generate the back off values without the jitter of an {@link ExponentialBackOff}.
	 * These values do not change between application starts, so they can be used to
	 * derive the retry topic names.
	 * @return the back off values without jitter.
	 * @since 4.2
	 */
	public List<Long> generateValuesWithoutJitter() {
		if (this.backOff instanceof ExponentialBackOff exponentialBackOff && exponentialBackOff.getJitter() > 0) {
			ExponentialBackOff backOffWithoutJitter =
					new ExponentialBackOff(exponentialBackOff.getInitialInterval(), exponentialBackOff.getMultiplier());
			backOffWithoutJitter.setMaxInterval(exponentialBackOff.getMaxInterval());
			backOffWithoutJitter.setMaxElapsedTime(exponentialBackOff.getMaxElapsedTime());
			backOffWithoutJitter.setMaxAttempts(exponentialBackOff.getMaxAttempts());
			return generateValues(backOffWithoutJitter);
		}
		return generateValues();
	}

	private List<Long> generateValues(BackOff backOffToUse) {
		BackOffExecution backOffExecution = backOffToUse.start();
		return Stream.generate(backOffExecution::nextBackOff)
				.takeWhile(value -> value != BackOffExecution.STOP)
				.limit(this.numberOfValuesToCreate)
				.toList();
	}

}
