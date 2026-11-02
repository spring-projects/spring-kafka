/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import java.lang.reflect.Method;

import org.jspecify.annotations.Nullable;

import org.springframework.core.CoroutinesUtils;
import org.springframework.core.KotlinDetector;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * An {@link InvocableHandlerMethod} extension for supporting Kotlin {@code suspend} function.
 *
 * @author Wang Zhiyang
 *
 * @since 3.2
 */
public class KotlinAwareInvocableHandlerMethod extends InvocableHandlerMethod {

	public KotlinAwareInvocableHandlerMethod(Object bean, Method method) {
		super(bean, method);
	}

	@Override
	@Nullable
	protected Object doInvoke(@Nullable Object... args) throws Exception {
		Method method = getBridgedMethod();
		if (KotlinDetector.isSuspendingFunction(method)) {
			return CoroutinesUtils.invokeSuspendingFunction(method, getBean(), args);
		}
		else {
			return super.doInvoke(args);
		}
	}

}
