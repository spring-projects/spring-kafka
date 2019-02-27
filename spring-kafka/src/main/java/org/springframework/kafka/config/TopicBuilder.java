/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

/**
 * Builder for a {@link NewTopic}.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public final class TopicBuilder {

	private final String name;

	private int partitions = 1;

	private short replicas = 1;

	private Map<Integer, List<Integer>> replicasAssignments;

	private final Map<String, String> configs = new HashMap<>();

	private TopicBuilder(String name) {
		this.name = name;
	}

	/**
	 * Set the number of partitions (default 1).
	 * @param partitions the partitions.
	 * @return the builder.
	 */
	public TopicBuilder partitions(int partitions) {
		this.partitions = partitions;
		return this;
	}

	/**
	 * Set the number of replicas (default 1).
	 * @param replicas the replicas (which will be cast to short).
	 * @return the builder.
	 */
	public TopicBuilder replicas(int replicas) {
		this.replicas = (short) replicas;
		return this;
	}

	/**
	 * Set the replica assignments.
	 * @param replicasAssignments the assignments.
	 * @return the builder.
	 * @see NewTopic#replicasAssignments()
	 */
	public TopicBuilder replicasAssignments(Map<Integer, List<Integer>> replicasAssignments) {
		this.replicasAssignments = replicasAssignments;
		return this;
	}

	/**
	 * Set the configs.
	 * @param configs the configs.
	 * @return the builder.
	 * @see NewTopic#configs()
	 */
	public TopicBuilder configs(Map<String, String> configs) {
		this.configs.putAll(configs);
		return this;
	}

	/**
	 * Set a configuration option.
	 * @param configName the name.
	 * @param configValue the value.
	 * @return the builder
	 * @see TopicConfig
	 */
	public TopicBuilder config(String configName, String configValue) {
		this.configs.put(configName, configValue);
		return this;
	}

	/**
	 * Set the {@link TopicConfig#CLEANUP_POLICY_CONFIG} to
	 * {@link TopicConfig#CLEANUP_POLICY_COMPACT}.
	 * @return the builder.
	 */
	public TopicBuilder compact() {
		this.configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
		return this;
	}

	public NewTopic build() {
		NewTopic topic = this.replicasAssignments == null
				? new NewTopic(this.name, this.partitions, this.replicas)
				: new NewTopic(this.name, this.replicasAssignments);
		if (this.configs.size() > 0) {
			topic.configs(this.configs);
		}
		return topic;
	}

	/**
	 * Create a TopicBuilder with the supplied name.
	 * @param name the name.
	 * @return the builder.
	 */
	public static TopicBuilder name(String name) {
		return new TopicBuilder(name);
	}

}
