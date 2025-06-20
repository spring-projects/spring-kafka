/*
 * Copyright 2017-present the original author or authors.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.CleanupConfig;
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
 * @author Julien Wittouck
 * @author Sanghyeok An
 * @author Cédric Schaller
 * @author Almog Gavra
 *
 * @since 1.1.4
 */
public class StreamsBuilderFactoryBean extends AbstractFactoryBean<StreamsBuilder>
		implements SmartLifecycle, BeanNameAware, SmartInitializingSingleton {

	/**
	 * The default {@link Duration} of {@code 10 seconds} for close timeout.
	 * @see KafkaStreams#close(Duration)
	 */
	public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(StreamsBuilderFactoryBean.class));

	private static final String STREAMS_CONFIG_MUST_NOT_BE_NULL = "'streamsConfig' must not be null";

	private static final String CLEANUP_CONFIG_MUST_NOT_BE_NULL = "'cleanupConfig' must not be null";

	private final ReentrantLock lifecycleLock = new ReentrantLock();

	private final List<Listener> listeners = new ArrayList<>();

	private KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

	@SuppressWarnings("NullAway.Init")
	private Properties properties;

	private CleanupConfig cleanupConfig;

	private KafkaStreamsInfrastructureCustomizer infrastructureCustomizer = new KafkaStreamsInfrastructureCustomizer() {
	};

	private KafkaStreamsCustomizer kafkaStreamsCustomizer = kafkaStreams -> { };

	@SuppressWarnings("NullAway.Init")
	private KafkaStreams.StateListener stateListener;

	private @Nullable  StateRestoreListener stateRestoreListener;

	private @Nullable StreamsUncaughtExceptionHandler streamsUncaughtExceptionHandler;

	private boolean autoStartup = true;

	private int phase = Integer.MAX_VALUE - 1000; // NOSONAR magic #

	private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private boolean leaveGroupOnClose = false;

	private @Nullable KafkaStreams kafkaStreams;

	private volatile boolean running;

	@SuppressWarnings("NullAway.Init")
	private Topology topology;

	@SuppressWarnings("NullAway.Init")
	private String beanName;

	/**
	 * Default constructor that creates the factory without configuration
	 * {@link Properties}. It is the factory user's responsibility to properly set
	 * {@link Properties} using
	 * {@link StreamsBuilderFactoryBean#setStreamsConfiguration(Properties)}.
	 * @since 2.1.3.
	 */
	@SuppressWarnings("NullAway.Init")
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

	@Override
	public synchronized void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Set the streams configuration {@link Properties} on this factory.
	 * @param streamsConfig the streams configuration.
	 * @since 2.2
	 */
	public void setStreamsConfiguration(Properties streamsConfig) {
		Assert.notNull(streamsConfig, STREAMS_CONFIG_MUST_NOT_BE_NULL);
		this.properties = streamsConfig;
	}

	@Nullable
	public Properties getStreamsConfiguration() {
		return this.properties; // NOSONAR - inconsistent synchronization
	}

	public void setClientSupplier(KafkaClientSupplier clientSupplier) {
		Assert.notNull(clientSupplier, "'clientSupplier' must not be null");
		this.clientSupplier = clientSupplier; // NOSONAR (sync)
	}

	/**
	 * Set a customizer to configure the builder and/or topology before creating the stream.
	 * @param infrastructureCustomizer the customizer
	 * @since 2.4.1
	 */
	public void setInfrastructureCustomizer(KafkaStreamsInfrastructureCustomizer infrastructureCustomizer) {
		Assert.notNull(infrastructureCustomizer, "'infrastructureCustomizer' must not be null");
		this.infrastructureCustomizer = infrastructureCustomizer; // NOSONAR (sync)
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

	/**
	 * Set a {@link StreamsUncaughtExceptionHandler}.
	 * @param streamsUncaughtExceptionHandler the handler.
	 * @since 2.8
	 */
	public void setStreamsUncaughtExceptionHandler(StreamsUncaughtExceptionHandler streamsUncaughtExceptionHandler) {
		this.streamsUncaughtExceptionHandler = streamsUncaughtExceptionHandler; // NOSONAR (sync)
	}

	/**
	 * Retrieves the current {@link StreamsUncaughtExceptionHandler} set on this factory bean.
	 * @return {@link StreamsUncaughtExceptionHandler}
	 * @since 2.8.4
	 */
	public @Nullable StreamsUncaughtExceptionHandler getStreamsUncaughtExceptionHandler() {
		return this.streamsUncaughtExceptionHandler;
	}

	public void setStateRestoreListener(StateRestoreListener stateRestoreListener) {
		this.stateRestoreListener = stateRestoreListener; // NOSONAR (sync)
	}

	/**
	 * Specify the timeout in seconds for the {@link KafkaStreams#close(Duration)}
	 * operation. Defaults to {@link #DEFAULT_CLOSE_TIMEOUT} seconds.
	 * @param closeTimeout the timeout for close in seconds.
	 * @see KafkaStreams#close(Duration)
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = Duration.ofSeconds(closeTimeout); // NOSONAR (sync)
	}

	/**
	 * Specify if the consumer should leave the group when stopping Kafka Streams. Defaults to false.
	 * @param leaveGroupOnClose true to leave the group when stopping the Streams
	 * @since 3.2.0
	 */
	public void setLeaveGroupOnClose(boolean leaveGroupOnClose) {
		this.leaveGroupOnClose = leaveGroupOnClose;
	}

	/**
	 * Providing access to the associated {@link Topology} of this
	 * {@link StreamsBuilderFactoryBean}.
	 * @return {@link Topology} object
	 * @since 2.4.4
	 */
	public Topology getTopology() {
		return this.topology;
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

	public void setCleanupConfig(CleanupConfig cleanupConfig) {
		Assert.notNull(cleanupConfig, CLEANUP_CONFIG_MUST_NOT_BE_NULL);
		this.cleanupConfig = cleanupConfig; // NOSONAR (sync)
	}

	/**
	 * Get a managed by this {@link StreamsBuilderFactoryBean} {@link KafkaStreams} instance.
	 * @return KafkaStreams managed instance;
	 * may be null if this {@link StreamsBuilderFactoryBean} hasn't been started.
	 * @since 1.1.4
	 */
	@Nullable
	public synchronized KafkaStreams getKafkaStreams() {
		this.lifecycleLock.lock();
		try {
			return this.kafkaStreams;
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	/**
	 * Get the current list of listeners.
	 * @return the listeners.
	 * @since 2.5.3
	 */
	public List<Listener> getListeners() {
		return Collections.unmodifiableList(this.listeners);
	}

	/**
	 * Add a {@link Listener} which will be called after starting and stopping the
	 * streams.
	 * @param listener the listener.
	 * @since 2.5.3
	 */
	public void addListener(Listener listener) {
		Assert.notNull(listener, "'listener' cannot be null");
		this.listeners.add(listener);
	}

	/**
	 * Remove a listener.
	 * @param listener the listener.
	 * @return true if removed.
	 * @since 2.5.3
	 */
	public boolean removeListener(Listener listener) {
		return this.listeners.remove(listener);
	}

	@Override
	protected StreamsBuilder createInstance() {
		this.lifecycleLock.lock();
		try {
			if (this.autoStartup) {
				Assert.state(this.properties != null,
						"streams configuration properties must not be null");
			}
			StreamsBuilder builder = createStreamBuilder();
			this.infrastructureCustomizer.configureBuilder(builder);
			return builder;
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	@Override
	public void start() {
		this.lifecycleLock.lock();
		try {
			if (!this.running) {
				try {
					Assert.state(this.properties != null,
							"streams configuration properties must not be null");
					this.kafkaStreams = this.kafkaStreamsCustomizer.initKafkaStreams(
							this.topology, this.properties, this.clientSupplier
					);
					this.kafkaStreams.setStateListener(this.stateListener);
					this.kafkaStreams.setGlobalStateRestoreListener(this.stateRestoreListener);
					if (this.streamsUncaughtExceptionHandler != null) {
						this.kafkaStreams.setUncaughtExceptionHandler(this.streamsUncaughtExceptionHandler);
					}
					this.kafkaStreamsCustomizer.customize(this.kafkaStreams);
					if (this.cleanupConfig.cleanupOnStart()) {
						this.kafkaStreams.cleanUp();
					}
					this.kafkaStreams.start();
					for (Listener listener : this.listeners) {
						listener.streamsAdded(this.beanName, this.kafkaStreams);
					}
					this.running = true;
				}
				catch (Exception e) {
					throw new KafkaException("Could not start stream: ", e);
				}
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public void stop() {
		this.lifecycleLock.lock();
		try {
			if (this.running) {
				try {
					if (this.kafkaStreams != null) {
						this.kafkaStreams.close(new KafkaStreams.CloseOptions()
								.timeout(this.closeTimeout)
								.leaveGroup(this.leaveGroupOnClose)
						);
						if (this.cleanupConfig.cleanupOnStop()) {
							this.kafkaStreams.cleanUp();
						}
						for (Listener listener : this.listeners) {
							listener.streamsRemoved(this.beanName, this.kafkaStreams);
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
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public boolean isRunning() {
		this.lifecycleLock.lock();
		try {
			return this.running;
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public void afterSingletonsInstantiated() {
		try {
			this.topology = getObject().build(this.properties);
			this.infrastructureCustomizer.configureTopology(this.topology);
			TopologyDescription description = this.topology.describe();
			LOGGER.debug(description::toString);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private StreamsBuilder createStreamBuilder() {
		if (this.properties == null) {
			return new StreamsBuilder();
		}
		else {
			StreamsConfig streamsConfig = new StreamsConfig(this.properties);
			TopologyConfig topologyConfig = new TopologyConfig(streamsConfig);
			return new StreamsBuilder(topologyConfig);
		}
	}

	/**
	 * Called whenever a {@link KafkaStreams} is added or removed.
	 *
	 * @since 2.5.3
	 *
	 */
	public interface Listener {

		/**
		 * A new {@link KafkaStreams} was created.
		 * @param id the streams id (factory bean name).
		 * @param streams the streams;
		 */
		default void streamsAdded(String id, KafkaStreams streams) {
		}

		/**
		 * An existing {@link KafkaStreams} was removed.
		 * @param id the streams id (factory bean name).
		 * @param streams the streams;
		 */
		default void streamsRemoved(String id, KafkaStreams streams) {
		}

	}

}
