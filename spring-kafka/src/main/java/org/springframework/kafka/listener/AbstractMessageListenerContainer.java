/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

/**
 *
 * @author Gary Russell
 */
public abstract class AbstractMessageListenerContainer<K, V>
		implements MessageListenerContainer, BeanNameAware, SmartLifecycle {

	protected final Log logger = LogFactory.getLog(this.getClass());

	public enum AckMode {
		/**
		 * Call {@link Consumer#commitAsync()} after each record is passed to the listener.
		 */
		RECORD,

		/**
		 * Call {@link Consumer#commitAsync()} after the results of each poll have been
		 * passed to the listener.
		 */
		BATCH,

		/**
		 * Call {@link Consumer#commitAsync()} for pending updates after
		 * {@link AbstractMessageListenerContainer#setAckTime(long) ackTime} has elapsed.
		 */
		TIME,

		/**
		 * Call {@link Consumer#commitAsync()} for pending updates after
		 * {@link AbstractMessageListenerContainer#setAckCount(int) ackCount} has been
		 * exceeded.
		 */
		COUNT,

		/**
		 * Call {@link Consumer#commitAsync()} for pending updates after
		 * {@link AbstractMessageListenerContainer#setAckCount(int) ackCount} has been
		 * exceeded or after {@link AbstractMessageListenerContainer#setAckTime(long)
		 * ackTime} has elapsed.
		 */
		COUNT_TIME,

		/**
		 * Same as {@link #COUNT_TIME} except for pending manual acks.
		 */
		MANUAL,

		/**
		 * Call {@link Consumer#commitAsync()} immediately for pending acks.
		 */
		MANUAL_IMMEDIATE

	}

	private final Object lifecycleMonitor = new Object();

	private String beanName;

	private AckMode ackMode = AckMode.BATCH;

	private int ackCount;

	private long ackTime;

	private Object messageListener;

	private volatile long pollTimeout = 1000;

	private boolean autoStartup = true;

	private int phase = 0;

	private volatile boolean running = false;

	private Executor taskExecutor;

	private ErrorHandler errorHandler = new LoggingErrorHandler();


	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	public String getBeanName() {
		return this.beanName;
	}

	/**
	 * Set the message listener; must be a {@link MessageListener} or
	 * {@link AcknowledgingMessageListener}.
	 * @param messageListener the listener.
	 */
	public void setMessageListener(Object messageListener) {
		Assert.isTrue(
				messageListener instanceof MessageListener || messageListener instanceof AcknowledgingMessageListener,
				"Either a " + MessageListener.class.getName() + " or a " + AcknowledgingMessageListener.class.getName()
						+ " must be provided");
		this.messageListener = messageListener;
	}

	public Object getMessageListener() {
		return this.messageListener;
	}

	@Override
	public void setupMessageListener(Object messageListener) {
		setMessageListener(messageListener);
	}

	/**
	 * The ack mode to use when auto ack (in the configuration properties) is
	 * false.
	 * <ul>
	 * <li>RECORD: Ack after each record has been passed to the listener.</li>
	 * <li>BATCH: Ack after each batch of records received from the consumer has
	 * been passed to the listener</li>
	 * <li>TIME: Ack after this number of milliseconds;
	 * (should be greater than {@code #setPollTimeout(long) pollTimeout}.</li>
	 * <li>COUNT: Ack after at least this number of records have been received</li>
	 * <li>MANUAL: Listener is responsible for acking - use a
	 * {@link AcknowledgingMessageListener}.
	 * </ul>
	 * @param ackMode the {@link AckMode}; default BATCH.
	 */
	public void setAckMode(AckMode ackMode) {
		this.ackMode = ackMode;
	}

	/**
	 * @return the {@link AckMode}
	 * @see #setAckMode(AckMode)
	 */
	public AckMode getAckMode() {
		return this.ackMode;
	}

	/**
	 * The max time to block in the consumer waiting for records.
	 * @param pollTimeout the timeout in ms; default 1000.
	 */
	public void setPollTimeout(long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	/**
	 * @return the poll timeout.
	 * @see #setPollTimeout(long)
	 */
	public long getPollTimeout() {
		return this.pollTimeout;
	}

	/**
	 * Set the number of outstanding record count after which offsets should be committed
	 * when {@link AckMode#COUNT} or {@link AckMode#COUNT_TIME} is being used.
	 * @param count the count
	 */
	public void setAckCount(int count) {
		this.ackCount = count;
	}

	/**
	 * @return the count.
	 * @see #setAckCount(int)
	 */
	public int getAckCount() {
		return this.ackCount;
	}

	/**
	 * Set the time (ms) after which outstanding offsets should be committed
	 * when {@link AckMode#TIME} or {@link AckMode#COUNT_TIME} is being used. Should
	 * be larger than
	 * @param millis the time
	 */
	public void setAckTime(long millis) {
		this.ackTime = millis;
	}

	/**
	 * @return the time.
	 * @see AbstractMessageListenerContainer#setAckTime(long)
	 */
	public long getAckTime() {
		return this.ackTime;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public final void start() {
		synchronized (this.lifecycleMonitor) {
			doStart();
		}
	}

	protected abstract void doStart();

	@Override
	public final void stop() {
		stop(null);
	}

	@Override
	public void stop(Runnable callback) {
		synchronized (this.lifecycleMonitor) {
			doStop();
		}
		if (callback != null) {
			callback.run();
		}
	}

	protected abstract void doStop();

	protected void setRunning(boolean running) {
		this.running = running;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	public ErrorHandler getErrorHandler() {
		return this.errorHandler;
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	public Executor getTaskExecutor() {
		return this.taskExecutor;
	}

	public void setTaskExecutor(Executor fetchTaskExecutor) {
		this.taskExecutor = fetchTaskExecutor;
	}

}
