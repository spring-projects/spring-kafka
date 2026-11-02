/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.DelegatingMessageListener;
import org.springframework.kafka.listener.ListenerType;
import org.springframework.kafka.listener.ListenerUtils;

/**
 * Top level class for all listener adapters.
 *
 * @param <T> the delegate type.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
public abstract class AbstractDelegatingMessageListenerAdapter<T>
		implements ConsumerSeekAware, DelegatingMessageListener<T> {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass())); // NOSONAR

	protected final T delegate; //NOSONAR

	protected final ListenerType delegateType; // NOSONAR

	private final @Nullable ConsumerSeekAware seekAware;

	public AbstractDelegatingMessageListenerAdapter(T delegate) {
		this.delegate = delegate;
		this.delegateType = ListenerUtils.determineListenerType(delegate);
		if (delegate instanceof ConsumerSeekAware) {
			this.seekAware = (ConsumerSeekAware) delegate;
		}
		else {
			this.seekAware = null;
		}
	}

	@Override
	public T getDelegate() {
		return this.delegate;
	}

	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		if (this.seekAware != null) {
			this.seekAware.registerSeekCallback(callback);
		}
	}

	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.seekAware != null) {
			this.seekAware.onPartitionsAssigned(assignments, callback);
		}
	}

	@Override
	public void onPartitionsRevoked(@Nullable Collection<TopicPartition> partitions) {
		if (this.seekAware != null) {
			this.seekAware.onPartitionsRevoked(partitions);
		}
	}

	@Override
	public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.seekAware != null) {
			this.seekAware.onIdleContainer(assignments, callback);
		}
	}

}
