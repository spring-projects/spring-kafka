/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.support;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.Nullable;

import org.springframework.util.Assert;

/**
 * A {@link ProducerListener} that delegates to a collection of listeners.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1.6
 *
 */
public class CompositeProducerListener<K, V> implements ProducerListener<K, V> {

	private final List<ProducerListener<K, V>> delegates = new CopyOnWriteArrayList<>();

	@SafeVarargs
	@SuppressWarnings("varargs")
	public CompositeProducerListener(ProducerListener<K, V>... delegates) {
		setDelegates(delegates);
	}

	@SafeVarargs
	@SuppressWarnings("varargs")
	public final void setDelegates(ProducerListener<K, V>... delegates) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates, "'delegates' cannot contain null elements");
		this.delegates.clear();
		this.delegates.addAll(Arrays.asList(delegates));
	}

	protected List<ProducerListener<K, V>> getDelegates() {
		return this.delegates;
	}

	public void addDelegate(ProducerListener<K, V> delegate) {
		this.delegates.add(delegate);
	}

	public boolean removeDelegate(ProducerListener<K, V> delegate) {
		return this.delegates.remove(delegate);
	}

	@Override
	public void onSuccess(ProducerRecord<K, V> producerRecord, RecordMetadata recordMetadata) {
		this.delegates.forEach(d -> d.onSuccess(producerRecord, recordMetadata));
	}

	@Override
	public void onError(ProducerRecord<K, V> producerRecord, @Nullable RecordMetadata recordMetadata, Exception exception) {
		this.delegates.forEach(d -> d.onError(producerRecord, recordMetadata, exception));
	}

}
