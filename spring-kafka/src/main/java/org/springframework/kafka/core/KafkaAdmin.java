/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.kafka.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;

/**
 * An admin that delegates to an {@link AdminClient} to create topics defined
 * in the application context.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
public class KafkaAdmin implements ApplicationContextAware, SmartLifecycle, DisposableBean {

	private static final int DEFAULT_CLOSE_TIMEOUT = 10;

	private final Log logger = LogFactory.getLog(KafkaAdmin.class);

	private final AdminClient adminClient;

	private boolean running;

	private int phase;

	private boolean autoStartup = true;

	private ApplicationContext applicationContext;

	private int closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	/**
	 * Create an instance with the provided {@link AdminClient}.
	 * @param adminClient the client.
	 */
	public KafkaAdmin(AdminClient adminClient) {
		this.adminClient = adminClient;
	}

	/**
	 * Create an instance with an {@link AdminClient} created from the supplied
	 * configuration.
	 * @param config the configuration.
	 */
	public KafkaAdmin(Map<String, Object> config) {
		this.adminClient = AdminClient.create(config);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public AdminClient getAdminClient() {
		return this.adminClient;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Set the close timeout in seconds. Defaults to 10 seconds.
	 * @param closeTimeout the timeout.
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			if (this.applicationContext != null) {
				addTopicsIfNeeded(this.applicationContext.getBeansOfType(NewTopic.class, false, false).values());
			}
			this.running = true;
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			this.running = false;
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	@Override
	public void destroy() throws Exception {
		this.adminClient.close(this.closeTimeout, TimeUnit.SECONDS);
	}

	private void addTopicsIfNeeded(Collection<NewTopic> topics) {
		if (topics.size() > 0) {
			Map<String, NewTopic> topicNameToTopic = new HashMap<>();
			topics.stream().forEach(t -> topicNameToTopic.compute(t.name(), (k, v) -> v = t));
			DescribeTopicsResult topicInfo = this.adminClient
					.describeTopics(topics.stream().map(t -> t.name()).collect(Collectors.toList()));
			List<NewTopic> topicsToAdd = new ArrayList<>();
			topicInfo.values().forEach((n, f) -> {
				try {
					TopicDescription topicDescription = f.get();
					if (topicNameToTopic.get(n).numPartitions() != topicDescription.partitions().size()) {
						if (logger.isDebugEnabled()) {
							logger.debug(String.format(
									"Topic '%s' exists but has a different partition count: %d not %d", n,
									topicDescription.partitions().size(), topicNameToTopic.get(n).numPartitions()));
						}
					}
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				catch (ExecutionException e) {
					topicsToAdd.add(topicNameToTopic.get(n));
				}
			});
			if (topicsToAdd.size() > 0) {
				CreateTopicsResult topicResults = this.adminClient.createTopics(topicsToAdd);
				try {
					topicResults.all().get();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.error("Interrupted while waiting for topic creation results", e);
				}
				catch (ExecutionException e) {
					logger.error("Failed to create topics", e.getCause());
				}
			}
		}
	}

}
