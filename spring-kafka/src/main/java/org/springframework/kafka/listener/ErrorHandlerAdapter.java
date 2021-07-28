/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.Assert;

/**
 * Adapts a legacy {@link ErrorHandler} or {@link BatchErrorHandler}.
 *
 * @author Gary Russell
 * @since 2.7.4
 *
 */
class ErrorHandlerAdapter implements CommonErrorHandler {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static final ConsumerRecords EMPTY_BATCH = new ConsumerRecords(Collections.emptyMap());

	private final ErrorHandler errorHandler;

	private final BatchErrorHandler batchErrorHandler;

	/**
	 * Adapt an {@link ErrorHandler}.
	 * @param errorHandler the handler.
	 */
	ErrorHandlerAdapter(ErrorHandler errorHandler) {
		Assert.notNull(errorHandler, "'errorHandler' cannot be null");
		this.errorHandler = errorHandler;
		this.batchErrorHandler = null;
	}

	/**
	 * Adapt a {@link BatchErrorHandler}.
	 * @param batchErrorHandler the handler.
	 */
	ErrorHandlerAdapter(BatchErrorHandler batchErrorHandler) {
		Assert.notNull(batchErrorHandler, "'batchErrorHandler' cannot be null");
		this.errorHandler = null;
		this.batchErrorHandler = batchErrorHandler;
	}

	@Override
	public boolean remainingRecords() {
		return this.errorHandler instanceof RemainingRecordsErrorHandler;
	}

	@Override
	public boolean deliveryAttemptHeader() {
		return this.errorHandler instanceof DeliveryAttemptAware;
	}

	@Override
	public void clearThreadState() {
		if (this.errorHandler != null) {
			this.errorHandler.clearThreadState();
		}
		else {
			this.batchErrorHandler.clearThreadState();
		}
	}

	@Override
	public boolean isAckAfterHandle() {
		if (this.errorHandler != null) {
			return this.errorHandler.isAckAfterHandle();
		}
		else {
			return this.batchErrorHandler.isAckAfterHandle();
		}
	}

	@Override
	public void setAckAfterHandle(boolean ack) {
		if (this.errorHandler != null) {
			this.errorHandler.setAckAfterHandle(ack);
		}
		else {
			this.batchErrorHandler.setAckAfterHandle(ack);
		}
	}

	@Override
	public int deliveryAttempt(TopicPartitionOffset topicPartitionOffset) {
		Assert.state(deliveryAttemptHeader(), "This method should not be called by the container");
		return ((DeliveryAttemptAware) this.errorHandler).deliveryAttempt(topicPartitionOffset);
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
			MessageListenerContainer container, boolean batchListener) {

		if (this.errorHandler != null) {
			this.errorHandler.handle(thrownException, Collections.EMPTY_LIST, consumer, container);
		}
		else {
			this.batchErrorHandler.handle(thrownException, EMPTY_BATCH, consumer, container, () -> { });
		}
	}

	@Override
	public void handleRecord(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		if (this.errorHandler != null) {
			this.errorHandler.handle(thrownException, record, consumer);
		}
		else {
			CommonErrorHandler.super.handleRecord(thrownException, record, consumer, container);
		}
	}

	@Override
	public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		if (this.errorHandler != null) {
			this.errorHandler.handle(thrownException, records, consumer, container);
		}
		else {
			CommonErrorHandler.super.handleRemaining(thrownException, records, consumer, container);
		}
	}

	@Override
	public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		if (this.batchErrorHandler != null) {
			this.batchErrorHandler.handle(thrownException, data, consumer, container, invokeListener);
		}
		else {
			CommonErrorHandler.super.handleBatch(thrownException, data, consumer, container, invokeListener);
		}
	}

}

