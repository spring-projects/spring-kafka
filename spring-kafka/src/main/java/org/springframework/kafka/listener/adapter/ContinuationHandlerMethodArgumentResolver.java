/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import reactor.core.publisher.Mono;

import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;

/**
 * No-op resolver for method arguments of type {@link kotlin.coroutines.Continuation}.
 * <p>
 * This class is similar to
 * {@link org.springframework.messaging.handler.annotation.reactive.ContinuationHandlerMethodArgumentResolver}
 * but for regular {@link HandlerMethodArgumentResolver} contract.
 *
 * @author Wang Zhiyang
 * @author Huijin Hong
 *
 * @since 3.2
 *
 * @see org.springframework.messaging.handler.annotation.reactive.ContinuationHandlerMethodArgumentResolver
 */
public class ContinuationHandlerMethodArgumentResolver implements HandlerMethodArgumentResolver {

	@Override
	public boolean supportsParameter(MethodParameter parameter) {
		return AdapterUtils.isKotlinContinuation(parameter.getParameterType());
	}

	@Override
	public Object resolveArgument(MethodParameter parameter, Message<?> message) {
		return Mono.empty();
	}

}
