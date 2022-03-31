/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.kafka.support.micrometer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.observation.MeterObservationHandler;
import io.micrometer.observation.Observation;

/**
 * A {@link MeterObservationHandler} for timers, cached using the {@code error} tag as the
 * key.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class CachingTimerObservationHandler implements MeterObservationHandler<Observation.Context> {

	private final MeterRegistry meterRegistry;

	private final Map<String, Timer> cache = new ConcurrentHashMap<>();

	/**
	 * Construct an instance with the provided registry.
	 * @param meterRegistry the registry.
	 */
	public CachingTimerObservationHandler(MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	@Override
	public void onStart(Observation.Context context) {
		Timer.Sample sample = Timer.start(this.meterRegistry);
		context.put(Timer.Sample.class, sample);
	}

	@Override
	public void onStop(Observation.Context context) {
		Timer.Sample sample = context.getRequired(Timer.Sample.class);
		String error = context.getError().map(th -> th.getClass().getName()).orElse("none");
		Timer timer = this.cache.computeIfAbsent(error, err -> Timer.builder(context.getName())
					.tag("error", err)
					.tags(Tags.of(
							context.getLowCardinalityTags().stream().map(tag -> Tag.of(tag.getKey(), tag.getValue()))
									.collect(Collectors.toList())))
					.register(this.meterRegistry));
			context.put(error, timer);
		sample.stop(timer);
	}

	@Override
	public boolean supportsContext(Observation.Context context) {
		return true;
	}

}
