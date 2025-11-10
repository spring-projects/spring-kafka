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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import org.springframework.core.retry.RetryPolicy;
import org.springframework.format.annotation.DurationFormat;
import org.springframework.format.datetime.standard.DurationFormatterUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Create a {@link org.springframework.util.backoff.BackOff BackOff} from the state of a
 * {@link BackOff @BackOff} annotation.
 *
 * @author Stephane Nicoll
 *
 * @since 4.0
 */
final class BackOffFactory {

	private static final long DEFAULT_DELAY = 1000;

	private final @Nullable StringValueResolver embeddedValueResolver;

	BackOffFactory(@Nullable StringValueResolver embeddedValueResolver) {
		this.embeddedValueResolver = embeddedValueResolver;
	}

	/**
	 * Create a {@link org.springframework.util.backoff.BackOff BackOff} instance based on
	 * the state of the given {@link BackOff @Backff}. The returned
	 * {@link org.springframework.util.backoff.BackOff BackOff} instance has unlimited
	 * number of attempts as these are controlled externally.
	 * @param annotation the annotation to source the parameters from
	 * @return a {@link org.springframework.util.backoff.BackOff}
	 */
	org.springframework.util.backoff.BackOff createFromAnnotation(BackOff annotation) {
		Duration delay = resolveDuration("delay", () -> annotation.delay() == DEFAULT_DELAY
				? annotation.value() : annotation.delay(), annotation::delayString);
		Duration maxDelay = resolveDuration("maxDelay", annotation::maxDelay, annotation::maxDelayString);
		double multiplier = resolveMultiplier(annotation);
		Duration jitter = resolveDuration("jitter", annotation::jitter, annotation::jitterString);
		if (maxDelay == Duration.ZERO && multiplier == 0 && jitter == Duration.ZERO) {
			Assert.isTrue(!delay.isNegative(),
					() -> "Invalid delay (%dms): must be >= 0.".formatted(delay.toMillis()));
			return new FixedBackOff(delay.toMillis());
		}
		RetryPolicy.Builder retryPolicyBuilder = RetryPolicy.builder().maxRetries(Long.MAX_VALUE);
		retryPolicyBuilder.delay(delay);
		if (maxDelay != Duration.ZERO) {
			retryPolicyBuilder.maxDelay(maxDelay);
		}
		if (multiplier != 0) {
			retryPolicyBuilder.multiplier(multiplier);
		}
		if (jitter != Duration.ZERO) {
			retryPolicyBuilder.jitter(jitter);
		}
		return retryPolicyBuilder.build().getBackOff();
	}

	private Duration resolveDuration(String attributeName, Supplier<@Nullable Long> valueRaw,
			Supplier<String> valueString) {

		String resolvedValue = resolve(valueString.get());
		if (StringUtils.hasLength(resolvedValue)) {
			try {
				return toDuration(resolvedValue, TimeUnit.MILLISECONDS);
			}
			catch (RuntimeException ex) {
				throw new IllegalArgumentException(
						"Invalid duration value for '%s': '%s'; %s".formatted(attributeName, resolvedValue, ex));
			}
		}
		Long raw = valueRaw.get();
		return (raw != null && raw != 0) ? Duration.ofMillis(raw) : Duration.ZERO;
	}

	private Double resolveMultiplier(BackOff annotation) {
		String resolvedMultiplier = resolve(annotation.multiplierString());
		if (StringUtils.hasLength(resolvedMultiplier)) {
			try {
				return Double.valueOf(resolvedMultiplier);
			}
			catch (NumberFormatException ex) {
				throw new IllegalArgumentException(
						"Invalid multiplier: '%s'; %s".formatted(resolvedMultiplier, ex));
			}
		}
		return annotation.multiplier();
	}

	private @Nullable String resolve(String valueString) {
		if (StringUtils.hasLength(valueString) && this.embeddedValueResolver != null) {
			return this.embeddedValueResolver.resolveStringValue(valueString);
		}
		return valueString;
	}

	private static Duration toDuration(String valueToResolve, TimeUnit timeUnit) {
		DurationFormat.Unit unit = DurationFormat.Unit.fromChronoUnit(timeUnit.toChronoUnit());
		return DurationFormatterUtils.detectAndParse(valueToResolve, unit);
	}

}
