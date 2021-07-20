/*
 * Copyright 2021 the original author or authors.
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An error handler that delegates to different error handlers, depending on the exception
 * type.
 *
 * @deprecated in favor of {@link CommonDelegatingErrorHandler}.
 *
 * @author Gary Russell
 * @since 2.7.4
 *
 */
@Deprecated
public class ConditionalDelegatingBatchErrorHandler implements ListenerInvokingBatchErrorHandler {

	private final ContainerAwareBatchErrorHandler defaultErrorHandler;

	private final Map<Class<? extends Throwable>, ContainerAwareBatchErrorHandler> delegates = new LinkedHashMap<>();

	/**
	 * Construct an instance with a default error handler that will be invoked if the
	 * exception has no matches.
	 * @param defaultErrorHandler the default error handler.
	 */
	public ConditionalDelegatingBatchErrorHandler(ContainerAwareBatchErrorHandler defaultErrorHandler) {
		Assert.notNull(defaultErrorHandler, "'defaultErrorHandler' cannot be null");
		this.defaultErrorHandler = defaultErrorHandler;
	}

	/**
	 * Set the delegate error handlers; a {@link LinkedHashMap} argument is recommended so
	 * that the delegates are searched in a known order.
	 * @param delegates the delegates.
	 */
	public void setErrorHandlers(Map<Class<? extends Throwable>, ContainerAwareBatchErrorHandler> delegates) {
		this.delegates.clear();
		this.delegates.putAll(delegates);
	}

	/**
	 * Add a delegate to the end of the current collection.
	 * @param throwable the throwable for this handler.
	 * @param handler the handler.
	 */
	public void addDelegate(Class<? extends Throwable> throwable, ContainerAwareBatchErrorHandler handler) {
		this.delegates.put(throwable, handler);
	}

	@Override
	public void handle(Exception thrownException, @Nullable ConsumerRecords<?, ?> records, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		// Never called but, just in case
		doHandle(thrownException, records, consumer, container, null);
	}

	@Override
	public void handle(Exception thrownException, @Nullable ConsumerRecords<?, ?> records, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		doHandle(thrownException, records, consumer, container, invokeListener);
	}

	protected void doHandle(Exception thrownException, @Nullable ConsumerRecords<?, ?> records, Consumer<?, ?> consumer,
			MessageListenerContainer container, @Nullable Runnable invokeListener) {

		Throwable cause = thrownException;
		if (cause instanceof ListenerExecutionFailedException) {
			cause = thrownException.getCause();
		}
		if (cause != null) {
			Class<? extends Throwable> causeClass = cause.getClass();
			for (Entry<Class<? extends Throwable>, ContainerAwareBatchErrorHandler> entry : this.delegates.entrySet()) {
				if (entry.getKey().isAssignableFrom(causeClass)) {
					entry.getValue().handle(thrownException, records, consumer, container, invokeListener);
					return;
				}
			}
		}
		this.defaultErrorHandler.handle(thrownException, records, consumer, container, invokeListener);
	}

}
