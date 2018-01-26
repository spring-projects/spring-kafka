/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * Async send/receive template.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 * @param <R> the reply data type.
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public class AsyncKafkaTemplate<K, V, R> extends KafkaTemplate<K, V> implements BatchMessageListener<K, R>,
		InitializingBean, SmartLifecycle {

	private static final long DEFAULT_REPLY_TIMEOUT = 5000L;

	private final GenericMessageListenerContainer<K, R> replyContainer;

	private final ConcurrentMap<String, RequestReplyFuture<K, V, R>> futures = new ConcurrentHashMap<>();

	private TaskScheduler scheduler;

	private boolean running;

	private int phase;

	private boolean autoStartup;

	private long replyTimeout = DEFAULT_REPLY_TIMEOUT;

	public AsyncKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, R> replyContainer) {
		this(producerFactory, replyContainer, false);
	}

	public AsyncKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, R> replyContainer, boolean autoFlush) {
		super(producerFactory, autoFlush);
		this.replyContainer = replyContainer;
		this.replyContainer.setupMessageListener(this);
	}

	protected void setTaskScheduler(TaskScheduler scheduler) {
		this.scheduler = scheduler;
	}

	protected void setReplyTimeout(long replyTimeout) {
		Assert.isTrue(replyTimeout >= 0, "'replyTimeout' must be >= 0");
		this.replyTimeout = replyTimeout;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.scheduler, "A scheduler is required for timeouts");
	}

	@Override
	public synchronized void start() {
		try {
			afterPropertiesSet();
		}
		catch (Exception e) {
			throw new KafkaException("Failed to initialize", e);
		}
		this.replyContainer.start();
		this.running = true;
	}

	@Override
	public synchronized void stop() {
		this.running = false;
		this.replyContainer.stop();
	}

	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	public RequestReplyFuture<K, V, R> sendAndReceive(ProducerRecord<K, V> record) {
		String correlationId = createCorrelationId();
		record.headers()
				.add(new RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8)));
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Sending: " + record + " with correlationId: " + correlationId);
		}
		RequestReplyFuture<K, V, R> future = new RequestReplyFuture<>();
		this.futures.put(correlationId, future);
		future.setSendFuture(send(record));
		this.scheduler.schedule(() -> {
			RequestReplyFuture<K, V, R> removed = this.futures.remove(correlationId);
			if (removed != null) {
				if (this.logger.isWarnEnabled()) {
					this.logger.warn("Reply timed out for: " + record + " with correlationId: " + correlationId);
				}
				removed.setException(new KafkaException("Reply timed out"));
			}
		}, Instant.now().plusMillis(this.replyTimeout));
		return future;
	}

	protected String createCorrelationId() {
		return UUID.randomUUID().toString();
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, R>> data) {
		data.forEach(record -> {
			Iterator<Header> iterator = record.headers().iterator();
			String correlationId = null;
			while (correlationId == null && iterator.hasNext()) {
				Header next = iterator.next();
				if (next.key().equals(KafkaHeaders.CORRELATION_ID)) {
					correlationId = new String(next.value(), StandardCharsets.UTF_8);
				}
			}
			if (correlationId == null) {
				this.logger.error("No correlationId found in reply: " + record);
			}
			else {
				RequestReplyFuture<K, V, R> future = this.futures.remove(correlationId);
				if (future == null) {
					this.logger.error("No pending reply: " + record + " with correlationId: "
							+ correlationId + ", perhaps timed out");
				}
				else {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Received: " + record + " with correlationId: " + correlationId);
					}
					future.set(record);
				}
			}
		});
	}

	/**
	 * A listenable future for requests/replies.
	 *
	 * @param <K> the key type.
	 * @param <V> the outbound data type.
	 * @param <R> the reply data type.
	 */
	public static class RequestReplyFuture<K, V, R> extends SettableListenableFuture<ConsumerRecord<K, R>> {

		private volatile ListenableFuture<SendResult<K, V>> sendFuture;

		RequestReplyFuture() {
			super();
		}

		void setSendFuture(ListenableFuture<SendResult<K, V>> sendFuture) {
			this.sendFuture = sendFuture;
		}

		public ListenableFuture<SendResult<K, V>> getSendFuture() {
			return this.sendFuture;
		}

	}

}
