/*
 * Copyright 2017-2021 the original author or authors.
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

import java.util.concurrent.Executor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.KafkaException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A container error handler that stops the container after an exception
 * is thrown by the listener.
 *
 * @deprecated in favor of {@link CommonContainerStoppingErrorHandler}.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
@Deprecated
public class ContainerStoppingBatchErrorHandler extends KafkaExceptionLogLevelAware
		implements ContainerAwareBatchErrorHandler {

	private final Executor executor;

	/**
	 * Construct an instance with a {@link SimpleAsyncTaskExecutor}.
	 */
	public ContainerStoppingBatchErrorHandler() {
		this.executor = new SimpleAsyncTaskExecutor();
	}

	/**
	 * Construct an instance with the provided {@link Executor}.
	 * @param executor the executor.
	 */
	public ContainerStoppingBatchErrorHandler(Executor executor) {
		Assert.notNull(executor, "'executor' cannot be null");
		this.executor = executor;
	}

	@Override
	public void handle(Exception thrownException, @Nullable ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		this.executor.execute(() -> container.stop());
		// isRunning is false before the container.stop() waits for listener thread
		int n = 0;
		while (container.isRunning() && n++ < 100) { // NOSONAR magic #
			try {
				Thread.sleep(100); // NOSONAR magic #
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		throw new KafkaException("Stopped container", getLogLevel(), thrownException);
	}

}
