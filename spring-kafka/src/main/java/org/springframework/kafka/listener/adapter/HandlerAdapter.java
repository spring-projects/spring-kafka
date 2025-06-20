/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.lang.reflect.Method;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.springframework.core.KotlinDetector;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * A wrapper for either an {@link InvocableHandlerMethod} or
 * {@link DelegatingInvocableHandler}. All methods delegate to the
 * underlying handler.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public class HandlerAdapter {

	private final @Nullable InvocableHandlerMethod invokerHandlerMethod;

	private final @Nullable DelegatingInvocableHandler delegatingHandler;

	private final boolean asyncReplies;

	/**
	 * Construct an instance with the provided method.
	 * @param invokerHandlerMethod the method.
	 */
	public HandlerAdapter(InvocableHandlerMethod invokerHandlerMethod) {
		this.invokerHandlerMethod = invokerHandlerMethod;
		this.delegatingHandler = null;
		Method handlerMethod = invokerHandlerMethod.getMethod();
		this.asyncReplies =
				AdapterUtils.isAsyncReply(handlerMethod.getReturnType())
						|| KotlinDetector.isSuspendingFunction(handlerMethod);
	}

	/**
	 * Construct an instance with the provided delegating handler.
	 * @param delegatingHandler the handler.
	 */
	public HandlerAdapter(DelegatingInvocableHandler delegatingHandler) {
		this.invokerHandlerMethod = null;
		this.delegatingHandler = delegatingHandler;
		this.asyncReplies = delegatingHandler.isAsyncReplies();
	}

	/**
	 * Return true if any handler method has an async reply type.
	 * @return the asyncReply.
	 * @since 3.2
	 */
	public boolean isAsyncReplies() {
		return this.asyncReplies;
	}

	@Nullable
	public Object invoke(Message<?> message, @Nullable Object... providedArgs) throws Exception { //NOSONAR
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.invoke(message, providedArgs); // NOSONAR
		}
		else if (Objects.requireNonNull(this.delegatingHandler).hasDefaultHandler()) {
			// Needed to avoid returning raw Message which matches Object
			Object[] args = new Object[providedArgs.length + 1];
			args[0] = message.getPayload();
			System.arraycopy(providedArgs, 0, args, 1, providedArgs.length);
			return this.delegatingHandler.invoke(message, args);
		}
		else {
			return this.delegatingHandler.invoke(message, providedArgs);
		}
	}

	public String getMethodAsString(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod().toGenericString();
		}
		else {
			return Objects.requireNonNull(this.delegatingHandler).getMethodNameFor(payload);
		}
	}

	public Object getBean() {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getBean();
		}
		else {
			return Objects.requireNonNull(this.delegatingHandler).getBean();
		}
	}

	@Nullable
	public InvocationResult getInvocationResultFor(Object result, @Nullable Object inboundPayload) {
		if (this.delegatingHandler != null && inboundPayload != null) {
			return this.delegatingHandler.getInvocationResultFor(result, inboundPayload);
		}
		return null;
	}

}
