/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * A {@link BatchInterceptor} that delegates to one or more {@link BatchInterceptor}s in
 * order.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Christian Fredriksson
 * @since 2.7
 *
 */
public class CompositeBatchInterceptor<K, V> implements BatchInterceptor<K, V> {

	private final Collection<BatchInterceptor<K, V>> delegates = new ArrayList<>();

	/**
	 * Construct an instance with the provided delegates.
	 * @param delegates the delegates.
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public CompositeBatchInterceptor(BatchInterceptor<K, V>... delegates) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates, "'delegates' cannot have null entries");
		this.delegates.addAll(Arrays.asList(delegates));
	}

	@Override
	public @Nullable ConsumerRecords<K, V> intercept(ConsumerRecords<K, V> records, Consumer<K, V> consumer) {
		ConsumerRecords<K, V> recordsToIntercept = records;
		for (BatchInterceptor<K, V> delegate : this.delegates) {
			recordsToIntercept = delegate.intercept(recordsToIntercept, consumer);
			if (recordsToIntercept == null) {
				break;
			}
		}
		return recordsToIntercept;
	}

	@Override
	public void success(ConsumerRecords<K, V> records, Consumer<K, V> consumer) {
		this.delegates.forEach(del -> del.success(records, consumer));
	}

	@Override
	public void failure(ConsumerRecords<K, V> records, Exception exception, Consumer<K, V> consumer) {
		this.delegates.forEach(del -> del.failure(records, exception, consumer));
	}

	@Override
	public void setupThreadState(Consumer<?, ?> consumer) {
		this.delegates.forEach(del -> del.setupThreadState(consumer));
	}

	@Override
	public void clearThreadState(Consumer<?, ?> consumer) {
		this.delegates.forEach(del -> del.clearThreadState(consumer));
	}

	/**
	 * Add an {@link BatchInterceptor} to delegates.
	 * @param batchInterceptor the interceptor.
	 * @since 4.0
	 */
	public void addBatchInterceptor(BatchInterceptor<K, V> batchInterceptor) {
		this.delegates.add(batchInterceptor);
	}

}
