/*
 * Copyright 2021-2022 the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.List;
import java.util.concurrent.Executor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.KafkaException;
import org.springframework.util.Assert;

/**
 * A {@link CommonErrorHandler} that stops the container when an error occurs. Replaces
 * the legacy {@link ContainerStoppingErrorHandler} and
 * {@link ContainerStoppingBatchErrorHandler}.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class CommonContainerStoppingErrorHandler extends KafkaExceptionLogLevelAware implements CommonErrorHandler {

	private final Executor executor;

	private boolean stopContainerAbnormally = true;

	/**
	 * Construct an instance with a default {@link SimpleAsyncTaskExecutor}.
	 */
	public CommonContainerStoppingErrorHandler() {
		this(new SimpleAsyncTaskExecutor("containerStop-"));
	}

	/**
	 * Construct an instance with the provided {@link Executor}.
	 * @param executor the executor.
	 */
	public CommonContainerStoppingErrorHandler(Executor executor) {
		Assert.notNull(executor, "'executor' cannot be null");
		this.executor = executor;
	}

	/**
	 * Set to false to stop the container normally. By default, the container is stopped
	 * abnormally, so that {@code container.isInExpectedState()} returns false. If you
	 * want to container to remain "healthy" when using this error handler, set the
	 * property to false.
	 * @param stopContainerAbnormally false for normal stop.
	 * @since 2.8
	 */
	public void setStopContainerAbnormally(boolean stopContainerAbnormally) {
		this.stopContainerAbnormally = stopContainerAbnormally;
	}

	@Override
	@Deprecated
	public boolean remainingRecords() {
		return true;
	}

	@Override
	public boolean seeksAfterHandling() {
		// We don't actually do any seeks here, but stopping the container has the same effect.
		return true;
	}

	@Override
	public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
			MessageListenerContainer container, boolean batchListener) {

		stopContainer(container, thrownException);
	}


	@Override
	public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		stopContainer(container, thrownException);
	}

	@Override
	public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		stopContainer(container, thrownException);
	}

	private void stopContainer(MessageListenerContainer container, Exception thrownException) {
		this.executor.execute(() -> {
			if (this.stopContainerAbnormally) {
				container.stopAbnormally(() -> {
				});
			}
			else {
				container.stop(() -> {
				});
			}
		});
		// isRunning is false before the container.stop() waits for listener thread
		try {
			ListenerUtils.stoppableSleep(container, 10_000); // NOSONAR
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		throw new KafkaException("Stopped container", getLogLevel(), thrownException);
	}

}
