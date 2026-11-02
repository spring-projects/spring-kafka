/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * Default implementation for {@link ListenerMetadata}.
 * @author Francois Rosiere
 * @author Ngoc Nhan
 * @since 2.8.6
 */
class DefaultListenerMetadata implements ListenerMetadata {

	private final MessageListenerContainer container;

	DefaultListenerMetadata(MessageListenerContainer container) {
		Assert.notNull(container, "'container' must not be null");
		this.container = container;
	}

	@Override
	@Nullable
	public String getListenerId() {
		return this.container.getListenerId();
	}

	@Override
	@Nullable
	public String getGroupId() {
		return this.container.getGroupId();
	}

	@Override
	public byte @Nullable [] getListenerInfo() {
		return this.container.getListenerInfo();
	}
}
