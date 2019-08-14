/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.kafka.config;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An {@link AbstractFactoryBean} for the {@link StreamsBuilder} instance
 * and lifecycle control for the internal {@link KafkaStreams} instance.
 * <p>
 * A fine grained control on {@link KafkaStreams} can be achieved by
 * {@link KafkaStreamsCustomizer}s.
 *
 * @author Artem Bilan
 * @author Ivan Ursul
 * @author Soby Chacko
 * @author Zach Olauson
 * @author Nurettin Yilmaz
 * @author Denis Washington
 * @author Gary Russell
 *
 * @since 1.1.4
 */
public class StreamsBuilderFactoryBean extends AbstractFactoryBean<StreamsBuilder> implements SmartLifecycle {

	/**
	 * The default {@link Duration} of {@code 10 seconds} for close timeout.
	 * @see KafkaStreams#close(Duration)
	 */
	public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(StreamsBuilderFactoryBean.class));

	private static final String STREAMS_CONFIG_MUST_NOT_BE_NULL = "'streamsConfig' must not be null";

	private static final String CLEANUP_CONFIG_MUST_NOT_BE_NULL = "'cleanupConfig' must not be null";

	private KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

	private Properties properties;

	private final CleanupConfig cleanupConfig;

	private KafkaStreamsCustomizer kafkaStreamsCustomizer;

	private KafkaStreams.StateListener stateListener;

	private StateRestoreListener stateRestoreListener;

	private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

	private boolean autoStartup = true;

	private int phase = Integer.MAX_VALUE - 1000; // NOSONAR magic #

	private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private KafkaStreams kafkaStreams;

	private volatile boolean running;

	/**
	 * Default constructor that creates the factory without configuration
	 * {@link Properties}. It is the factory user's responsibility to properly set
	 * {@link Properties} using
	 * {@link StreamsBuilderFactoryBean#setStreamsConfiguration(Properties)}.
	 * @since 2.1.3.
	 */
	public StreamsBuilderFactoryBean() {
		this.cleanupConfig = new CleanupConfig();
	}

	/**
	 * Construct an instance with the supplied streams configuration and
	 * clean up configuration.
	 * @param streamsConfig the streams configuration.
	 * @param cleanupConfig the cleanup configuration.
	 * @since 2.2
	 */
	public StreamsBuilderFactoryBean(KafkaStreamsConfiguration streamsConfig, CleanupConfig cleanupConfig) {
		Assert.notNull(streamsConfig, STREAMS_CONFIG_MUST_NOT_BE_NULL);
		Assert.notNull(cleanupConfig, CLEANUP_CONFIG_MUST_NOT_BE_NULL);
		this.properties = streamsConfig.asProperties();
		this.cleanupConfig = cleanupConfig;
	}

	/**
	 * Construct an instance with the supplied streams configuration.
	 * @param streamsConfig the streams configuration.
	 * @since 2.2
	 */
	public StreamsBuilderFactoryBean(KafkaStreamsConfiguration streamsConfig) {
		this(streamsConfig, new CleanupConfig());
	}

	/**
	 * Set {@link StreamsConfig} on this factory.
	 * @param streamsConfig the streams configuration.
	 * @since 2.2
	 */
	public void setStreamsConfiguration(Properties streamsConfig) {
		Assert.notNull(streamsConfig, STREAMS_CONFIG_MUST_NOT_BE_NULL);
		this.properties = streamsConfig;
	}

	@Nullable
	public Properties getStreamsConfiguration() {
		return this.properties;
	}

	public void setClientSupplier(KafkaClientSupplier clientSupplier) {
		Assert.notNull(clientSupplier, "'clientSupplier' must not be null");
		this.clientSupplier = clientSupplier; // NOSONAR (sync)
	}

	/**
	 * Specify a {@link KafkaStreamsCustomizer} to customize a {@link KafkaStreams}
	 * instance during {@link #start()}.
	 * @param kafkaStreamsCustomizer the {@link KafkaStreamsCustomizer} to use.
	 * @since 2.1.5
	 */
	public void setKafkaStreamsCustomizer(KafkaStreamsCustomizer kafkaStreamsCustomizer) {
		Assert.notNull(kafkaStreamsCustomizer, "'kafkaStreamsCustomizer' must not be null");
		this.kafkaStreamsCustomizer = kafkaStreamsCustomizer; // NOSONAR (sync)
	}

	public void setStateListener(KafkaStreams.StateListener stateListener) {
		this.stateListener = stateListener; // NOSONAR (sync)
	}

	public void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
		this.uncaughtExceptionHandler = exceptionHandler; // NOSONAR (sync)
	}

	public void setStateRestoreListener(StateRestoreListener stateRestoreListener) {
		this.stateRestoreListener = stateRestoreListener; // NOSONAR (sync)
	}

	/**
	 * Specify the timeout in seconds for the {@link KafkaStreams#close(Duration)} operation.
	 * Defaults to {@link #DEFAULT_CLOSE_TIMEOUT} seconds.
	 * @param closeTimeout the timeout for close in seconds.
	 * @see KafkaStreams#close(Duration)
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = Duration.ofSeconds(closeTimeout); // NOSONAR (sync)
	}

	@Override
	public Class<?> getObjectType() {
		return StreamsBuilder.class;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	/**
	 * Get a managed by this {@link StreamsBuilderFactoryBean} {@link KafkaStreams} instance.
	 * @return KafkaStreams managed instance;
	 * may be null if this {@link StreamsBuilderFactoryBean} hasn't been started.
	 * @since 1.1.4
	 */
	public KafkaStreams getKafkaStreams() {
		return this.kafkaStreams;
	}

	@Override
	protected synchronized StreamsBuilder createInstance() {
		if (this.autoStartup) {
			Assert.state(this.properties != null,
					"streams configuration properties must not be null");
		}
		return new StreamsBuilder();
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		if (callback != null) {
			callback.run();
		}
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			try {
				Assert.state(this.properties != null,
						"streams configuration properties must not be null");
				Topology topology = getObject().build(this.properties); // NOSONAR: getObject() cannot return null
				LOGGER.debug(() -> topology.describe().toString());
				this.kafkaStreams = new KafkaStreams(topology, this.properties, this.clientSupplier);
				this.kafkaStreams.setStateListener(this.stateListener);
				this.kafkaStreams.setGlobalStateRestoreListener(this.stateRestoreListener);
				this.kafkaStreams.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);
				if (this.kafkaStreamsCustomizer != null) {
					this.kafkaStreamsCustomizer.customize(this.kafkaStreams);
				}
				if (this.cleanupConfig.cleanupOnStart()) {
					this.kafkaStreams.cleanUp();
				}
				this.kafkaStreams.start();
				this.running = true;
			}
			catch (Exception e) {
				throw new KafkaException("Could not start stream: ", e);
			}
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			try {
				if (this.kafkaStreams != null) {
					this.kafkaStreams.close(this.closeTimeout);
					if (this.cleanupConfig.cleanupOnStop()) {
						this.kafkaStreams.cleanUp();
					}
					this.kafkaStreams = null;
				}
			}
			catch (Exception e) {
				LOGGER.error(e, "Failed to stop streams");
			}
			finally {
				this.running = false;
			}
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

	private Properties propertiesFromConfigs(Map<String, Object> configs) {
		Properties props = new Properties();
		props.putAll(configs);
		return props;
	}

}
