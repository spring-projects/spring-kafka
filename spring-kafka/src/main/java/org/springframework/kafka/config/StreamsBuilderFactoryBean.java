/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.config;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.CleanupConfig;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An {@link AbstractFactoryBean} for the {@link StreamsBuilder} instance
 * and lifecycle control for the internal {@link KafkaStreams} instance.
 *
 * <p>A fine grained control on {@link KafkaStreams} can be achieved by
 * {@link KafkaStreamsCustomizer}s</p>
 *
 * @author Artem Bilan
 * @author Ivan Ursul
 * @author Soby Chacko
 * @author Zach Olauson
 * @author Nurettin Yilmaz
 *
 * @since 1.1.4
 */
public class StreamsBuilderFactoryBean extends AbstractFactoryBean<StreamsBuilder> implements SmartLifecycle {

	private static final Log logger = LogFactory.getLog(StreamsBuilderFactoryBean.class); // NOSONAR

	private static final String STREAMS_CONFIG_MUST_NOT_BE_NULL = "'streamsConfig' must not be null";

	private static final String CLEANUP_CONFIG_MUST_NOT_BE_NULL = "'cleanupConfig' must not be null";

	private static final int DEFAULT_CLOSE_TIMEOUT = 10;

	private KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

	private StreamsConfig streamsConfig;

	private Properties properties;

	private final CleanupConfig cleanupConfig;

	private KafkaStreamsCustomizer kafkaStreamsCustomizer;

	private KafkaStreams.StateListener stateListener;

	private StateRestoreListener stateRestoreListener;

	private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

	private boolean autoStartup = true;

	private int phase = Integer.MAX_VALUE - 1000; // NOSONAR magic #

	private int closeTimeout = DEFAULT_CLOSE_TIMEOUT;

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
	 * Construct an instance with the supplied streams configuration.
	 * @param streamsConfig the streams configuration.
	 * @deprecated in favor of {@link #StreamsBuilderFactoryBean(KafkaStreamsConfiguration)}
	 */
	@Deprecated
	public StreamsBuilderFactoryBean(StreamsConfig streamsConfig) {
		this(streamsConfig, new CleanupConfig());
	}

	/**
	 * Construct an instance with the supplied streams configuration and
	 * clean up configuration.
	 * @param streamsConfig the streams configuration.
	 * @param cleanupConfig the cleanup configuration.
	 * @since 2.1.2.
	 * @deprecated in favor of {@link #StreamsBuilderFactoryBean(KafkaStreamsConfiguration, CleanupConfig)}
	 */
	@Deprecated
	public StreamsBuilderFactoryBean(StreamsConfig streamsConfig, CleanupConfig cleanupConfig) {
		Assert.notNull(streamsConfig, STREAMS_CONFIG_MUST_NOT_BE_NULL);
		Assert.notNull(cleanupConfig, CLEANUP_CONFIG_MUST_NOT_BE_NULL);
		this.streamsConfig = streamsConfig;
		this.cleanupConfig = cleanupConfig;
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
	 * @deprecated in favor of {@link #StreamsBuilderFactoryBean(KafkaStreamsConfiguration)}.
	 */
	@Deprecated
	public StreamsBuilderFactoryBean(Map<String, Object> streamsConfig) {
		this(streamsConfig, new CleanupConfig());
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
	 * Construct an instance with the supplied streams configuration and
	 * clean up configuration.
	 * @param streamsConfig the streams configuration.
	 * @param cleanupConfig the cleanup configuration.
	 * @since 2.1.2.
	 * @deprecated in favor of {@link #StreamsBuilderFactoryBean(KafkaStreamsConfiguration, CleanupConfig)}.
	 */
	@Deprecated
	public StreamsBuilderFactoryBean(Map<String, Object> streamsConfig, CleanupConfig cleanupConfig) {
		Assert.notNull(streamsConfig, STREAMS_CONFIG_MUST_NOT_BE_NULL);
		Assert.notNull(cleanupConfig, CLEANUP_CONFIG_MUST_NOT_BE_NULL);
		this.streamsConfig = new StreamsConfig(streamsConfig);
		this.cleanupConfig = cleanupConfig;
	}

	/**
	 * Set {@link StreamsConfig} on this factory.
	 * @param streamsConfig the streams configuration.
	 * @since 2.1.3
	 */
	public void setStreamsConfig(StreamsConfig streamsConfig) {
		Assert.notNull(streamsConfig, STREAMS_CONFIG_MUST_NOT_BE_NULL);
		Assert.isNull(this.properties, "Cannot have both streamsConfig and streams configuration properties");
		this.streamsConfig = streamsConfig;
	}

	@Nullable
	public StreamsConfig getStreamsConfig() {
		return this.streamsConfig;
	}

	/**
	 * Set {@link StreamsConfig} on this factory.
	 * @param streamsConfig the streams configuration.
	 * @since 2.2
	 */
	public void setStreamsConfiguration(Properties streamsConfig) {
		Assert.notNull(streamsConfig, STREAMS_CONFIG_MUST_NOT_BE_NULL);
		Assert.isNull(this.streamsConfig, "Cannot have both streamsConfig and streams configuration properties");
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
	 * Specify the timeout in seconds for the {@link KafkaStreams#close(long, TimeUnit)} operation.
	 * Defaults to {@value #DEFAULT_CLOSE_TIMEOUT} seconds.
	 * @param closeTimeout the timeout for close in seconds.
	 * @see KafkaStreams#close(long, TimeUnit)
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = closeTimeout; // NOSONAR (sync)
	}

	@Override
	public Class<?> getObjectType() {
		return StreamsBuilder.class;
	}

	@Override
	protected StreamsBuilder createInstance() {
		if (this.autoStartup) {
			Assert.state(this.streamsConfig != null || this.properties != null,
					"'streamsConfig' or streams configuration properties must not be null");
		}
		return new StreamsBuilder();
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public void setPhase(int phase) {
		this.phase = phase;
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

	@SuppressWarnings("deprecation")
	@Override
	public synchronized void start() {
		if (!this.running) {
			try {
				Assert.state(this.streamsConfig != null || this.properties != null,
						"'streamsConfig' or streams configuration properties must not be null");
				Topology topology = getObject().build(); // NOSONAR
				if (logger.isDebugEnabled()) {
					logger.debug(topology.describe());
				}
				if (this.properties != null) {
					this.kafkaStreams = new KafkaStreams(topology, this.properties, this.clientSupplier);
				}
				else {
					this.kafkaStreams = new KafkaStreams(topology, this.streamsConfig, this.clientSupplier);
				}
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
					this.kafkaStreams.close(this.closeTimeout, TimeUnit.SECONDS);
					if (this.cleanupConfig.cleanupOnStop()) {
						this.kafkaStreams.cleanUp();
					}
					this.kafkaStreams = null;
				}
			}
			catch (Exception e) {
				logger.error("Failed to stop streams", e);
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

}
