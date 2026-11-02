/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

/**
 * Message handler adapter implementing this interface can detect {@link HandlerAdapter} async return types.
 *
 * @author Wang zhiyang
 *
 * @since 3.2
 */
public interface AsyncRepliesAware {

	/**
	 * Return true if the {@link HandlerAdapter} return type is async.
	 * @return true for async replies.
	 * @since 3.2
	 */
	default boolean isAsyncReplies() {
		return false;
	}

}
