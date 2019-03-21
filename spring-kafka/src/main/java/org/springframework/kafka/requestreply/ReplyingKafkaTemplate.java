/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.kafka.requestreply;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * A KafkaTemplate that implements request/reply semantics.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 * @param <R> the reply data type.
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public class ReplyingKafkaTemplate<K, V, R> extends KafkaTemplate<K, V> implements BatchMessageListener<K, R>,
		InitializingBean, SmartLifecycle, DisposableBean, ReplyingKafkaOperations<K, V, R> {

	private static final long DEFAULT_REPLY_TIMEOUT = 5000L;

	private final GenericMessageListenerContainer<K, R> replyContainer;

	private final ConcurrentMap<CorrelationKey, RequestReplyFuture<K, V, R>> futures = new ConcurrentHashMap<>();

	private TaskScheduler scheduler = new ThreadPoolTaskScheduler();

	private int phase;

	private boolean autoStartup = true;

	private long replyTimeout = DEFAULT_REPLY_TIMEOUT;

	private volatile boolean schedulerSet;

	private volatile boolean running;

	public ReplyingKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, R> replyContainer) {
		this(producerFactory, replyContainer, false);
	}

	public ReplyingKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, R> replyContainer, boolean autoFlush) {
		super(producerFactory, autoFlush);
		Assert.notNull(replyContainer, "'replyContainer' cannot be null");
		this.replyContainer = replyContainer;
		this.replyContainer.setupMessageListener(this);
	}

	public void setTaskScheduler(TaskScheduler scheduler) {
		Assert.notNull(scheduler, "'scheduler' cannot be null");
		this.scheduler = scheduler;
		this.schedulerSet = true;
	}

	public void setReplyTimeout(long replyTimeout) {
		Assert.isTrue(replyTimeout >= 0, "'replyTimeout' must be >= 0");
		this.replyTimeout = replyTimeout;
	}

	@Override
	public boolean isRunning() {
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

	/**
	 * Return the topics/partitions assigned to the replying listener container.
	 * @return the topics/partitions.
	 */
	public Collection<TopicPartition> getAssignedReplyTopicPartitions() {
		return this.replyContainer.getAssignedPartitions();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (!this.schedulerSet) {
			((ThreadPoolTaskScheduler) this.scheduler).initialize();
		}
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			try {
				afterPropertiesSet();
			}
			catch (Exception e) {
				throw new KafkaException("Failed to initialize", e);
			}
			this.replyContainer.start();
			this.running = true;
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			this.running = false;
			this.replyContainer.stop();
			this.futures.clear();
		}
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	@Override
	public RequestReplyFuture<K, V, R> sendAndReceive(ProducerRecord<K, V> record) {
		Assert.state(this.running, "Template has not been start()ed"); // NOSONAR (sync)
		CorrelationKey correlationId = createCorrelationId(record);
		Assert.notNull(correlationId, "the created 'correlationId' cannot be null");
		record.headers().add(new RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.getCorrelationId()));
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Sending: " + record + " with correlationId: " + correlationId);
		}
		TemplateRequestReplyFuture<K, V, R> future = new TemplateRequestReplyFuture<>();
		this.futures.put(correlationId, future);
		try {
			future.setSendFuture(send(record));
		}
		catch (Exception e) {
			this.futures.remove(correlationId);
			throw new KafkaException("Send failed", e);
		}
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

	@Override
	public void destroy() throws Exception {
		if (!this.schedulerSet) {
			((ThreadPoolTaskScheduler) this.scheduler).destroy();
		}
	}

	/**
	 * Subclasses can override this to generate custom correlation ids.
	 * The default implementation is a 16 byte representation of a UUID.
	 * @param record the record.
	 * @return the key.
	 */
	protected CorrelationKey createCorrelationId(ProducerRecord<K, V> record) {
		UUID uuid = UUID.randomUUID();
		byte[] bytes = new byte[16];
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return new CorrelationKey(bytes);
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, R>> data) {
		data.forEach(record -> {
			Iterator<Header> iterator = record.headers().iterator();
			CorrelationKey correlationId = null;
			while (correlationId == null && iterator.hasNext()) {
				Header next = iterator.next();
				if (next.key().equals(KafkaHeaders.CORRELATION_ID)) {
					correlationId = new CorrelationKey(next.value());
				}
			}
			if (correlationId == null) {
				this.logger.error("No correlationId found in reply: " + record
						+ " - to use request/reply semantics, the responding server must return the correlation id "
						+ " in the '" + KafkaHeaders.CORRELATION_ID + "' header");
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
	 *
	 */
	public static class TemplateRequestReplyFuture<K, V, R> extends RequestReplyFuture<K, V, R> {

		TemplateRequestReplyFuture() {
			super();
		}

		@Override
		protected void setSendFuture(ListenableFuture<SendResult<K, V>> sendFuture) {
			super.setSendFuture(sendFuture);
		}

	}

}
