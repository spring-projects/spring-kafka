/*
 * Copyright 2024-present the original author or authors.
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

package org.springframework.kafka.core;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.jspecify.annotations.Nullable;

import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * An abstract class to manage {@link KafkaClientMetrics}.
 *
 * @param <C> the Kafka Client type.
 *
 * @author Artem Bilan
 *
 * @since 3.3
 *
 * @see KafkaClientMetrics
 */
public abstract class KafkaMetricsSupport<C> {

	protected final MeterRegistry meterRegistry;

	protected final List<Tag> tags;

	@Nullable
	protected final ScheduledExecutorService scheduler;

	private final Map<String, MeterBinder> metrics = new HashMap<>();

	/**
	 * Construct an instance with the provided registry.
	 * @param meterRegistry the registry.
	 */
	protected KafkaMetricsSupport(MeterRegistry meterRegistry) {
		this(meterRegistry, Collections.emptyList());
	}

	/**
	 * Construct an instance with the provided {@link MeterRegistry} and {@link TaskScheduler}.
	 * @param meterRegistry the registry.
	 * @param taskScheduler the task scheduler.
	 */
	protected KafkaMetricsSupport(MeterRegistry meterRegistry, TaskScheduler taskScheduler) {
		this(meterRegistry, Collections.emptyList(), taskScheduler);
	}

	/**
	 * Construct an instance with the provided {@link MeterRegistry} and tags.
	 * @param meterRegistry the registry.
	 * @param tags          the tags.
	 */
	protected KafkaMetricsSupport(MeterRegistry meterRegistry, List<Tag> tags) {
		Assert.notNull(meterRegistry, "The 'meterRegistry' cannot be null");
		this.meterRegistry = meterRegistry;
		this.tags = tags;
		this.scheduler = null;
	}

	/**
	 * Construct an instance with the provided {@link MeterRegistry}, tags and {@link TaskScheduler}.
	 * @param meterRegistry the registry.
	 * @param tags          the tags.
	 * @param taskScheduler the task scheduler.
	 */
	protected KafkaMetricsSupport(MeterRegistry meterRegistry, List<Tag> tags, TaskScheduler taskScheduler) {
		Assert.notNull(meterRegistry, "The 'meterRegistry' cannot be null");
		Assert.notNull(taskScheduler, "The 'taskScheduler' cannot be null");
		this.meterRegistry = meterRegistry;
		this.tags = tags;
		this.scheduler = obtainScheduledExecutorService(taskScheduler);
	}

	/**
	 * Bind metrics for the Apache Kafka client with provided id.
	 * @param id the unique identifier for the client to manage in store.
	 * @param client the Kafka client instance to bind.
	 */
	protected final void bindClient(String id, C client) {
		if (!this.metrics.containsKey(id)) {
			List<Tag> clientTags = new ArrayList<>(this.tags);
			clientTags.add(new ImmutableTag("spring.id", id));
			this.metrics.put(id, createClientMetrics(client, clientTags));
			this.metrics.get(id).bindTo(this.meterRegistry);
		}
	}

	/**
	 * Create a {@code io.micrometer.core.instrument.binder.kafka.KafkaMetrics} instance
	 * for the provided Kafka client and metric tags.
	 * By default, this factory is aware of {@link Consumer}, {@link Producer} and {@link AdminClient} types.
	 * For other use-case this method can be overridden.
	 * @param client the client to create a {@code io.micrometer.core.instrument.binder.kafka.KafkaMetrics} instance for.
	 * @param tags the tags for the {@code io.micrometer.core.instrument.binder.kafka.KafkaMetrics}.
	 * @return the {@code io.micrometer.core.instrument.binder.kafka.KafkaMetrics}.
	 */
	protected MeterBinder createClientMetrics(C client, List<Tag> tags) {
		if (client instanceof Consumer<?, ?> consumer) {
			return createConsumerMetrics(consumer, tags);
		}
		else if (client instanceof Producer<?, ?> producer) {
			return createProducerMetrics(producer, tags);
		}
		else if (client instanceof AdminClient admin) {
			return createAdminMetrics(admin, tags);
		}

		throw new IllegalArgumentException("Unsupported client type: " + client.getClass());
	}

	private KafkaClientMetrics createConsumerMetrics(Consumer<?, ?> consumer, List<Tag> tags) {
		return this.scheduler != null
				? new KafkaClientMetrics(consumer, tags, this.scheduler)
				: new KafkaClientMetrics(consumer, tags);
	}

	private KafkaClientMetrics createProducerMetrics(Producer<?, ?> producer, List<Tag> tags) {
		return this.scheduler != null
				? new KafkaClientMetrics(producer, tags, this.scheduler)
				: new KafkaClientMetrics(producer, tags);
	}

	private KafkaClientMetrics createAdminMetrics(AdminClient adminClient, List<Tag> tags) {
		return this.scheduler != null
				? new KafkaClientMetrics(adminClient, tags, this.scheduler)
				: new KafkaClientMetrics(adminClient, tags);
	}

	/**
	 * Unbind a {@code io.micrometer.core.instrument.binder.kafka.KafkaMetrics} for the provided Kafka client.
	 * @param id the unique identifier for the client to manage in store.
	 * @param client the Kafka client instance to unbind.
	 */
	protected final void unbindClient(@Nullable String id, C client) {
		AutoCloseable removed = (AutoCloseable) this.metrics.remove(id);
		if (removed != null) {
			try {
				removed.close();
			}
			catch (Exception ex) {
				ReflectionUtils.rethrowRuntimeException(ex);
			}
		}
	}

	private static ScheduledExecutorService obtainScheduledExecutorService(TaskScheduler taskScheduler) {
		if (taskScheduler instanceof ThreadPoolTaskScheduler threadPoolTaskScheduler) {
			return threadPoolTaskScheduler.getScheduledExecutor();
		}

		return new ScheduledExecutorServiceAdapter(taskScheduler);
	}

	private static final class ScheduledExecutorServiceAdapter extends ScheduledThreadPoolExecutor {

		private final TaskScheduler delegate;

		private ScheduledExecutorServiceAdapter(TaskScheduler delegate) {
			super(0);
			this.delegate = delegate;
		}

		@Override
		public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
				long initialDelay,
				long period,
				TimeUnit unit) {

			return this.delegate.scheduleAtFixedRate(command,
					Instant.now().plus(initialDelay, unit.toChronoUnit()),
					Duration.of(period, unit.toChronoUnit()));
		}

	}

}
