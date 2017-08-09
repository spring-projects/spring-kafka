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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * An admin that delegates to an {@link AdminClient} to create topics defined
 * in the application context.
 *
 * @author Gary Russell
 *
 * @since 1.3
 */
public class KafkaAdmin implements ApplicationContextAware, SmartInitializingSingleton {

	private static final int DEFAULT_CLOSE_TIMEOUT = 10;

	private final Log logger = LogFactory.getLog(KafkaAdmin.class);

	private final Map<String, Object> config;

	private ApplicationContext applicationContext;

	private int closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private boolean fatalIfBrokerNotAvailable;

	/**
	 * Create an instance with an {@link AdminClient} based on the supplied
	 * configuration.
	 * @param config the configuration for the {@link AdminClient}.
	 */
	public KafkaAdmin(Map<String, Object> config) {
		this.config = new HashMap<>(config);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * Set the close timeout in seconds. Defaults to 10 seconds.
	 * @param closeTimeout the timeout.
	 */
	public void setCloseTimeout(int closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

	/**
	 * Set to true if you want the application context to fail to load if we are unable
	 * to connect to the broker during initialization, to check/add topics.
	 * @param fatalIfBrokerNotAvailable true to fail.
	 */
	public void setFatalIfBrokerNotAvailable(boolean fatalIfBrokerNotAvailable) {
		this.fatalIfBrokerNotAvailable = fatalIfBrokerNotAvailable;
	}

	/**
	 * Get an unmodifiable copy of this admin's configuration.
	 * @return the configuration map.
	 */
	public Map<String, Object> getConfig() {
		return Collections.unmodifiableMap(this.config);
	}

	@Override
	public void afterSingletonsInstantiated() {
		if (!initialize()) {
			if (this.fatalIfBrokerNotAvailable) {
				throw new IllegalStateException("Could not configure topics");
			}
		}
	}

	/**
	 * Call this method to check/add topics; this might be needed if the broker was not
	 * available when the application context was initialized, and
	 * {@link #setFatalIfBrokerNotAvailable(boolean) fatalIfBrokenNotAvailable} is false.
	 * @return true if successful.
	 */
	public final boolean initialize() {
		AdminClient adminClient = null;
		try {
			adminClient = AdminClient.create(this.config);
		}
		catch (Exception e) {
			logger.error("Failed to create AdminClient", e);
		}
		if (adminClient != null) {
			try {
				if (this.applicationContext != null) {
					addTopicsIfNeeded(adminClient,
							this.applicationContext.getBeansOfType(NewTopic.class, false, false).values());
				}
				return true;
			}
			finally {
				adminClient.close(this.closeTimeout, TimeUnit.SECONDS);
			}
		}
		return false;
	}

	private void addTopicsIfNeeded(AdminClient adminClient, Collection<NewTopic> topics) {
		if (topics.size() > 0) {
			final List<String> names = new ArrayList<>();
			final Map<String, NewTopic> topicNameToTopic = new HashMap<>();
			for (NewTopic topic : topics) {
				names.add(topic.name());
				topicNameToTopic.put(topic.name(), topic);
			}
			DescribeTopicsResult topicInfo = adminClient.describeTopics(names);
			final List<NewTopic> topicsToAdd = new ArrayList<>();
			for (Entry<String, KafkaFuture<TopicDescription>> entry : topicInfo.values().entrySet()) {
				String n = entry.getKey();
				try {
					TopicDescription topicDescription = entry.getValue().get();
					if (topicNameToTopic.get(n).numPartitions() != topicDescription.partitions().size()) {
						if (logger.isInfoEnabled()) {
							logger.info(String.format(
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
			}
			if (topicsToAdd.size() > 0) {
				CreateTopicsResult topicResults = adminClient.createTopics(topicsToAdd);
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
