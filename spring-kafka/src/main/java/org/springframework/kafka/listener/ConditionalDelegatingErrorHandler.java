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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An error handler that delegates to different error handlers, depending on the exception
 * type.
 *
 * @author Gary Russell
 * @since 2.7.4
 *
 */
public class ConditionalDelegatingErrorHandler implements ContainerAwareErrorHandler {

	private final ContainerAwareErrorHandler defaultErrorHandler;

	private final Map<Class<? extends Throwable>, ContainerAwareErrorHandler> delegates = new LinkedHashMap<>();

	/**
	 * Construct an instance with a default error handler that will be invoked if the
	 * exception has no matches.
	 * @param defaultErrorHandler the default error handler.
	 */
	public ConditionalDelegatingErrorHandler(ContainerAwareErrorHandler defaultErrorHandler) {
		Assert.notNull(defaultErrorHandler, "'defaultErrorHandler' cannot be null");
		this.defaultErrorHandler = defaultErrorHandler;
	}

	/**
	 * Set the delegate error handlers; a {@link LinkedHashMap} argument is recommended so
	 * that the delegates are searched in a known order.
	 * @param delegates the delegates.
	 */
	public void setErrorHandlers(Map<Class<? extends Throwable>, ContainerAwareErrorHandler> delegates) {
		this.delegates.clear();
		this.delegates.putAll(delegates);
	}

	/**
	 * Add a delegate to the end of the current collection.
	 * @param throwable the throwable for this handler.
	 * @param handler the handler.
	 */
	public void addDelegate(Class<? extends Throwable> throwable, ContainerAwareErrorHandler handler) {
		this.delegates.put(throwable, handler);
	}

	@Override
	public void handle(Exception thrownException, @Nullable List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		boolean handled = false;
		Throwable cause = thrownException;
		if (cause instanceof ListenerExecutionFailedException) {
			cause = thrownException.getCause();
		}
		if (cause != null) {
			Class<? extends Throwable> causeClass = cause.getClass();
			for (Entry<Class<? extends Throwable>, ContainerAwareErrorHandler> entry : this.delegates.entrySet()) {
				if (entry.getKey().isAssignableFrom(causeClass)) {
					handled = true;
					entry.getValue().handle(thrownException, records, consumer, container);
					return;
				}
			}
		}
		this.defaultErrorHandler.handle(thrownException, records, consumer, container);
	}

}
