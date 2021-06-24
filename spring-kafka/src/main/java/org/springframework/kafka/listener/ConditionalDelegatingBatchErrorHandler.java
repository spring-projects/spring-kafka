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

import org.springframework.util.Assert;

/**
 * An error handler that delegates to different error handlers, depending on the exception
 * type.
 *
 * @author Gary Russell
 * @since 2.7.4
 *
 */
public class ConditionalDelegatingBatchErrorHandler implements ContainerAwareBatchErrorHandler {

	private final ContainerAwareBatchErrorHandler defaultErrorHandler;

	private final Map<Class<? extends Throwable>, ContainerAwareBatchErrorHandler> delegates = new LinkedHashMap<>();

	/**
	 * Construct an instance with a default error handler that will be invoked if the
	 * exception has no matches.
	 * @param defaultErrorHandler
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
	public void handle(Exception thrownException, ConsumerRecords<?, ?> records, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		// Never called but, just in case

		boolean handled = false;
		Throwable cause = thrownException.getCause();
		if (cause != null) {
			Class<? extends Throwable> causeClass = cause.getClass();
			for (Entry<Class<? extends Throwable>, ContainerAwareBatchErrorHandler> entry : this.delegates.entrySet()) {
				if (entry.getKey().equals(causeClass)) {
					handled = true;
					entry.getValue().handle(thrownException, records, consumer, container);
					break;
				}
			}
		}
		if (!handled) {
			this.defaultErrorHandler.handle(thrownException, records, consumer, container);
		}

	}

	@Override
	public void handle(Exception thrownException, ConsumerRecords<?, ?> records, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		boolean handled = false;
		Throwable cause = thrownException.getCause();
		if (cause != null) {
			Class<? extends Throwable> causeClass = cause.getClass();
			for (Entry<Class<? extends Throwable>, ContainerAwareBatchErrorHandler> entry : this.delegates.entrySet()) {
				if (entry.getKey().equals(causeClass)) {
					handled = true;
					entry.getValue().handle(thrownException, records, consumer, container, invokeListener);
					break;
				}
			}
		}
		if (!handled) {
			this.defaultErrorHandler.handle(thrownException, records, consumer, container, invokeListener);
		}
	}

}
