/*
 * Copyright 2017 the original author or authors.
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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.KafkaException;
import org.springframework.util.Assert;

/**
 * An {@link AbstractFactoryBean} for the {@link KStreamBuilder} instance
 * and lifecycle control for the internal {@link KafkaStreams} instance.
 *
 * @author Artem Bilan
 * @author Ivan Ursul
 *
 * @since 1.1.4
 */
public class KStreamBuilderFactoryBean extends AbstractFactoryBean<KStreamBuilder> implements SmartLifecycle {

	private static final int DEFAULT_CLOSE_TIMEOUT = 10;

	private final StreamsConfig streamsConfig;

	private KafkaStreams kafkaStreams;

	private KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

	private boolean autoStartup = true;

	private int phase = Integer.MIN_VALUE;

	private KafkaStreams.StateListener stateListener;

	private Thread.UncaughtExceptionHandler exceptionHandler;

	private int closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private volatile boolean running;

	public KStreamBuilderFactoryBean(StreamsConfig streamsConfig) {
		Assert.notNull(streamsConfig, "'streamsConfig' must not be null");
		this.streamsConfig = streamsConfig;
	}

	public KStreamBuilderFactoryBean(Map<String, Object> streamsConfig) {
		Assert.notNull(streamsConfig, "'streamsConfig' must not be null");
		this.streamsConfig = new StreamsConfig(streamsConfig);
	}

	public void setClientSupplier(KafkaClientSupplier clientSupplier) {
		Assert.notNull(clientSupplier, "'clientSupplier' must not be null");
		this.clientSupplier = clientSupplier;
	}

	public void setStateListener(KafkaStreams.StateListener stateListener) {
		this.stateListener = stateListener;
	}

	public void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	/**
	 * Specify the timeout in seconds for the {@link KafkaStreams#close(long, TimeUnit)} operation.
	 * Defaults to 10 seconds.
	 * @param closeTimeout the timeout for close in seconds.
	 * @see KafkaStreams#close(long, TimeUnit)
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

	@Override
	public Class<?> getObjectType() {
		return KStreamBuilder.class;
	}

	@Override
	protected KStreamBuilder createInstance() throws Exception {
		return new KStreamBuilder();
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

	@Override
	public synchronized void start() {
		if (!this.running) {
			try {
				this.kafkaStreams = new KafkaStreams(getObject(), this.streamsConfig, this.clientSupplier);
				this.kafkaStreams.setStateListener(this.stateListener);
				this.kafkaStreams.setUncaughtExceptionHandler(this.exceptionHandler);
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
					this.kafkaStreams.cleanUp();
					this.kafkaStreams = null;
				}
			}
			catch (Exception e) {
				e.printStackTrace();
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
	 * Get a managed by this {@link KStreamBuilderFactoryBean} {@link KafkaStreams} instance.
	 * @return KafkaStreams managed instance;
	 * may be null if this {@link KStreamBuilderFactoryBean} hasn't been started.
	 * @since 1.1.4
	 */
	public KafkaStreams getKafkaStreams() {
		return this.kafkaStreams;
	}

}
