/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.springframework.kafka.listener.adapter.AsyncRepliesAware;

/**
 * Classes implementing this interface allow containers to determine the type of the
 * ultimate listener.
 * <p>
 * This interface also implements {@link AsyncRepliesAware} contract delegating to the {@link #getDelegate()},
 * respectivelly.
 *
 * @param <T> the type received by the listener.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0
 *
 */
public interface DelegatingMessageListener<T> extends AsyncRepliesAware {

	/**
	 * Return the delegate.
	 * @return the delegate.
	 */
	T getDelegate();

	@Override
	default boolean isAsyncReplies() {
		return getDelegate() instanceof AsyncRepliesAware asyncRepliesAware && asyncRepliesAware.isAsyncReplies();
	}

}
