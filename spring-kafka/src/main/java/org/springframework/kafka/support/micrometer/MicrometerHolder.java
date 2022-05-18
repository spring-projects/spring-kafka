/*
 * Copyright 2020-2022 the original author or authors.
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

import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.lang.Nullable;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Builder;
import io.micrometer.core.instrument.Timer.Sample;

/**
 * A wrapper for micrometer timers when available on the class path.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public final class MicrometerHolder {

	private static final String NONE_EXCEPTION_METERS_KEY = "none";

	private final Map<String, Timer> meters = new ConcurrentHashMap<>();

	private final MeterRegistry registry;

	private final String timerName;

	private final String timerDesc;

	private final String name;

	private final Map<String, String> tags;

	/**
	 * Create an instance with the provided properties.
	 * @param context the application context from which to obtain the meter registry.
	 * @param name the value of the 'name' tag.
	 * @param timerName the timer name.
	 * @param timerDesc the timer description.
	 * @param tags additional tags.
	 */
	public MicrometerHolder(@Nullable ApplicationContext context, String name,
			String timerName, String timerDesc, Map<String, String> tags) {

		if (context == null) {
			throw new IllegalStateException("No micrometer registry present");
		}
		try {
			this.registry = context.getBeanProvider(MeterRegistry.class).getIfUnique();
		}
		catch (NoUniqueBeanDefinitionException ex) {
			throw new IllegalStateException(ex);
		}
		if (this.registry != null) {
			this.timerName = timerName;
			this.timerDesc = timerDesc;
			this.name = name;
			this.tags = tags;
			buildTimer(NONE_EXCEPTION_METERS_KEY);
		}
		else {
			throw new IllegalStateException("No micrometer registry present (or more than one and "
					+ "there is not exactly one marked with @Primary)");
		}
	}

	/**
	 * Start the timer.
	 * @return the sample.
	 */
	public Object start() {
		return Timer.start(this.registry);
	}

	/**
	 * Record success.
	 * @param sample the sample.
	 * @see #start()
	 */
	public void success(Object sample) {
		Timer timer = this.meters.get(NONE_EXCEPTION_METERS_KEY);
		if (timer != null) {
			((Sample) sample).stop(timer);
		}
	}

	/**
	 * Record failure.
	 * @param sample the sample.
	 * @param exception the exception name.
	 * @see #start()
	 */
	public void failure(Object sample, String exception) {
		Timer timer = this.meters.get(exception);
		if (timer == null) {
			timer = buildTimer(exception);
		}
		((Sample) sample).stop(timer);
	}

	private Timer buildTimer(String exception) {
		Builder builder = Timer.builder(this.timerName)
			.description(this.timerDesc)
			.tag("name", this.name)
			.tag("result", exception.equals(NONE_EXCEPTION_METERS_KEY) ? "success" : "failure")
			.tag("exception", exception);
		if (this.tags != null && !this.tags.isEmpty()) {
			this.tags.forEach(builder::tag);
		}
		Timer registeredTimer = builder.register(this.registry);
		this.meters.put(exception, registeredTimer);
		return registeredTimer;
	}

	/**
	 * Remove the timers.
	 */
	public void destroy() {
		this.meters.values().forEach(this.registry::remove);
		this.meters.clear();
	}

}
