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

package org.springframework.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.format.annotation.DurationFormat;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Collects metadata for creating a {@link org.springframework.util.backoff.BackOff BackOff}
 * instance as part of a {@link RetryPolicy}. Values can be provided as is or using a
 * {@code *String} equivalent that supports more format, as well as expression evaluations.
 * <p>
 * The available attributes lead to the following:
 * <ul>
 * <li>With no explicit settings, the default is a {@link FixedBackOff} with a delay of
 * {@value #DEFAULT_DELAY} ms</li>
 * <li>With only {@link #delay()} set: a fixed delay back off with that value</li>
 * <li>In all other cases, an {@link ExponentialBackOff} is created with the values of
 * {@link #delay()} (default: {@value RetryPolicy.Builder#DEFAULT_DELAY} ms),
 * {@link #maxDelay()} (default: no maximum), {@link #multiplier()}
 * (default: {@value RetryPolicy.Builder#DEFAULT_MULTIPLIER}) and {@link #jitter()}
 * (default: no jitter).</li>
 * </ul>
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Aftab Shaikh
 * @author Stephane Nicoll
 *
 * @since 4.0
 */
@Target(ElementType.ANNOTATION_TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface BackOff {

	/**
	 * Default {@link #delay()} in milliseconds.
	 */
	long DEFAULT_DELAY = 1000;

	/**
	 * Alias for {@link #delay()}.
	 * <p>Intended to be used when no other attributes are needed, for example:
	 * {@code @BackOff(2000)}.
	 *
	 * @return the based delay in milliseconds (default{@value DEFAULT_DELAY})
	 */
	@AliasFor("delay")
	long value() default DEFAULT_DELAY;

	/**
	 * Specify the base delay after the initial invocation.
	 * <p>If only a {@code delay} is specified, a {@link FixedBackOff} with that value
	 * as the interval is configured.
	 * <p>If a {@linkplain #multiplier() multiplier} is specified, this serves as the
	 * initial delay to multiply from.
	 * <p>The default is {@value DEFAULT_DELAY} milliseconds.
	 *
	 * @return the based delay in milliseconds (default{@value DEFAULT_DELAY})
	 */
	@AliasFor("value")
	long delay() default DEFAULT_DELAY;

	/**
	 * Specify the base delay after the initial invocation using a String format. If
	 * this is specified, takes precedence over {@link #delay()}.
	 * <p>The delay String can be in several formats:
	 * <ul>
	 * <li>a plain long &mdash; which is interpreted to represent a duration in
	 * milliseconds</li>
	 * <li>any of the known {@link DurationFormat.Style}: the {@link DurationFormat.Style#ISO8601 ISO8601}
	 * style or the {@link DurationFormat.Style#SIMPLE SIMPLE} style &mdash; using
	 * milliseconds as fallback if the string doesn't contain an explicit unit</li>
	 * <li>Regular expressions, such as {@code ${example.property}} to use the
	 * {@code example.property} from the environment</li>
	 * </ul>
	 *
	 * @return the based delay as a String value &mdash; for example a placeholder
	 * @see #delay()
	 */
	String delayString() default "";

	/**
	 * Specify the maximum delay for any retry attempt, limiting how far
	 * {@linkplain #jitter jitter} and the {@linkplain #multiplier() multiplier} can
	 * increase the {@linkplain #delay() delay}.
	 * <p>Ignored if only {@link #delay()} is set, otherwise an {@link ExponentialBackOff}
	 * with the given max delay or an unlimited delay if not set.
	 *
	 * @return the maximum delay
	 */
	long maxDelay() default 0;

	/**
	 * Specify the maximum delay for any retry attempt using a String format. If this is
	 * specified, takes precedence over {@link #maxDelay()}..
	 * <p>The max delay String can be in several formats:
	 * <ul>
	 * <li>a plain long &mdash; which is interpreted to represent a duration in
	 * milliseconds</li>
	 * <li>any of the known {@link DurationFormat.Style}: the {@link DurationFormat.Style#ISO8601 ISO8601}
	 * style or the {@link DurationFormat.Style#SIMPLE SIMPLE} style &mdash; using
	 * milliseconds as fallback if the string doesn't contain an explicit unit</li>
	 * <li>Regular expressions, such as {@code ${example.property}} to use the
	 * {@code example.property} from the environment</li>
	 * </ul>
	 *
	 * @return the max delay as a String value &mdash; for example a placeholder
	 * @see #maxDelay()
	 */
	String maxDelayString() default "";

	/**
	 * Specify a multiplier for a delay for the next retry attempt, applied to the previous
	 * delay, starting with the initial {@linkplain #delay() delay} as well as to the
	 * applicable {@linkplain #jitter() jitter} for each attempt.
	 * <p>Ignored if only {@link #delay()} is set, otherwise an {@link ExponentialBackOff}
	 * with the given multiplier or {@code 1.0} if not set.
	 *
	 * @return the value to multiply the current interval by for each attempt
	 */
	double multiplier() default 0;

	/**
	 * Specify a multiplier for a delay for the next retry attempt using a String format.
	 * If this is specified, takes precedence over {@link #multiplier()}.
	 * <p>The multiplier String can be in several formats:
	 * <ul>
	 * <li>a plain double</li>
	 * <li>Regular expressions, such as {@code ${example.property}} to use the
	 * {@code example.property} from the environment</li>
	 * </ul>
	 *
	 * @return the value to multiply the current interval by for each attempt &mdash;
	 * for example, a placeholder
	 * @see #multiplier()
	 */
	String multiplierString() default "";

	/**
	 * Specify a jitter value for the base retry attempt, randomly subtracted or added to
	 * the calculated delay, resulting in a value between {@code delay - jitter} and
	 * {@code delay + jitter} but never below the {@linkplain #delay() base delay} or
	 * above the {@linkplain #maxDelay() max delay}.
	 * <p>If a {@linkplain #multiplier() multiplier} is specified, it is applied to the
	 * jitter value as well.
	 * <p>Ignored if only {@link #delay()} is set, otherwise an {@link ExponentialBackOff}
	 * with the given jitter or no jitter if not set.
	 *
	 * @return the jitter value in milliseconds
	 * @see #delay()
	 * @see #maxDelay()
	 * @see #multiplier()
	 */
	long jitter() default 0;

	/**
	 * Specify a jitter value for the base retry attempt using a String format. If this is
	 * specified, takes precedence over {@link #jitter()}.
	 * <p>The jitter String can be in several formats:
	 * <ul>
	 * <li>a plain long &mdash; which is interpreted to represent a duration in
	 * milliseconds</li>
	 * <li>any of the known {@link DurationFormat.Style}: the {@link DurationFormat.Style#ISO8601 ISO8601}
	 * style or the {@link DurationFormat.Style#SIMPLE SIMPLE} style &mdash; using
	 * milliseconds as fallback if the string doesn't contain an explicit unit</li>
	 * <li>Regular expressions, such as {@code ${example.property}} to use the
	 * {@code example.property} from the environment</li>
	 * </ul>
	 *
	 * @return the jitter as a String value &mdash; for example, a placeholder
	 * @see #jitter()
	 */
	String jitterString() default "";

}
