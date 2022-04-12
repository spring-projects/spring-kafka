/*
 * Copyright 2021-2022 the original author or authors.
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

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.Assert;

/**
 * A {@link CommonErrorHandler} that delegates to different {@link CommonErrorHandler}s
 * for record and batch listeners.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class CommonMixedErrorHandler implements CommonErrorHandler {

	private final CommonErrorHandler recordErrorHandler;

	private final CommonErrorHandler batchErrorHandler;

	/**
	 * Construct an instance with the provided delegate {@link CommonErrorHandler}s.
	 * @param recordErrorHandler the error handler for record listeners.
	 * @param batchErrorHandler the error handler for batch listeners.
	 */
	public CommonMixedErrorHandler(CommonErrorHandler recordErrorHandler, CommonErrorHandler batchErrorHandler) {
		Assert.notNull(recordErrorHandler, "'recordErrorHandler' cannot be null");
		Assert.notNull(recordErrorHandler, "'batchErrorHandler' cannot be null");
		this.recordErrorHandler = recordErrorHandler;
		this.batchErrorHandler = batchErrorHandler;
	}

	@SuppressWarnings("deprecation")
	@Override
	public boolean remainingRecords() {
		return this.recordErrorHandler.remainingRecords();
	}

	@Override
	public boolean seeksAfterHandling() {
		return this.recordErrorHandler.seeksAfterHandling();
	}

	@Override
	public boolean deliveryAttemptHeader() {
		return this.recordErrorHandler.deliveryAttemptHeader();
	}

	@Override
	public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
			MessageListenerContainer container, boolean batchListener) {
		if (batchListener) {
			this.batchErrorHandler.handleOtherException(thrownException, consumer, container, batchListener);
		}
		else {
			this.recordErrorHandler.handleOtherException(thrownException, consumer, container, batchListener);
		}
	}

	@Override
	public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		return this.recordErrorHandler.handleOne(thrownException, record, consumer, container);
	}

	@Override
	public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		this.recordErrorHandler.handleRemaining(thrownException, records, consumer, container);
	}

	@Override
	public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		this.batchErrorHandler.handleBatch(thrownException, data, consumer, container, invokeListener);
	}

	@Override
	public int deliveryAttempt(TopicPartitionOffset topicPartitionOffset) {
		return this.recordErrorHandler.deliveryAttempt(topicPartitionOffset);
	}

	@Override
	public void clearThreadState() {
		this.batchErrorHandler.clearThreadState();
		this.recordErrorHandler.clearThreadState();
	}

	@Override
	public boolean isAckAfterHandle() {
		return this.recordErrorHandler.isAckAfterHandle();
	}

	@Override
	public void setAckAfterHandle(boolean ack) {
		this.batchErrorHandler.setAckAfterHandle(ack);
		this.recordErrorHandler.setAckAfterHandle(ack);
	}

}
