/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import org.jspecify.annotations.Nullable;

import org.springframework.expression.Expression;

/**
 * The result of a method invocation.
 * @param result the result.
 * @param messageReturnType the message return type.
 * @param sendTo the expression about sends topic.
 *
 * @author Gary Russell
 * @since 2.2
 */
public record InvocationResult(@Nullable Object result, @Nullable Expression sendTo, boolean messageReturnType) {

	@Override
	public String toString() {
		return "InvocationResult [result=" + this.result
				+ ", sendTo=" + (this.sendTo == null ? "null" : this.sendTo.getExpressionString())
				+ ", messageReturnType=" + this.messageReturnType + "]";
	}

}
