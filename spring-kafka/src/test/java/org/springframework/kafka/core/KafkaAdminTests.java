/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.kafka.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * @author Gary Russell
 * @since 1.3
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class KafkaAdminTests {

	@Autowired
	private KafkaAdmin admin;

	@Autowired
	private NewTopic topic1;

	@Autowired
	private NewTopic topic2;

	@Autowired
	private NewTopic topic3;

	@Test
	public void testTopicConfigs() {
		assertThat(topic1.configs()).containsEntry(
				TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
		assertThat(topic2.replicasAssignments())
			.isEqualTo(Collections.singletonMap(0, Collections.singletonList(0)));
		assertThat(topic2.configs()).containsEntry(
				TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd");
		assertThat(TopicBuilder.name("foo")
					.replicas(3)
					.build().replicationFactor()).isEqualTo((short) 3);
		assertThat(topic3.replicasAssignments()).hasSize(3);
	}

	@Test
	public void testAddTopicsAndAddPartitions() throws Exception {
		AdminClient adminClient = AdminClient.create(this.admin.getConfig());
		DescribeTopicsResult topics = adminClient.describeTopics(Arrays.asList("foo", "bar"));
		Map<String, TopicDescription> results = topics.all().get();
		results.forEach((name, td) -> assertThat(td.partitions()).hasSize(name.equals("foo") ? 2 : 1));
		new DirectFieldAccessor(this.topic1).setPropertyValue("numPartitions", Optional.of(4));
		new DirectFieldAccessor(this.topic2).setPropertyValue("numPartitions", Optional.of(3));
		this.admin.initialize();
		int n = 0;
		TopicDescription bar = results.values().stream()
			.filter(td -> td.name().equals("bar"))
			.findFirst()
			.get();
		while (n++ < 100 && bar.partitions().size() == 1) {
			Thread.sleep(100);
			topics = adminClient.describeTopics(Arrays.asList("foo", "bar"));
			results = topics.all().get();
			bar = results.values().stream()
					.filter(tp -> tp.name().equals("bar"))
					.findFirst()
					.get();
		}
		results.forEach((name, td) -> assertThat(td.partitions()).hasSize(name.equals("foo") ? 4 : 3));
		adminClient.close(Duration.ofSeconds(10));
	}

	@Test
	public void alreadyExists() throws Exception {
		AtomicReference<Method> addTopics = new AtomicReference<>();
		AtomicReference<Method> modifyTopics = new AtomicReference<>();
		ReflectionUtils.doWithMethods(KafkaAdmin.class, m -> {
			m.setAccessible(true);
			if (m.getName().equals("addTopics")) {
				addTopics.set(m);
			}
			else if (m.getName().equals("modifyTopics")) {
				modifyTopics.set(m);
			}
		}, m -> {
			return m.getName().endsWith("Topics");
		});
		try (AdminClient adminClient = AdminClient.create(this.admin.getConfig())) {
			addTopics.get().invoke(this.admin, adminClient, Collections.singletonList(this.topic1));
			modifyTopics.get().invoke(this.admin, adminClient, Collections.singletonMap(
					this.topic1.name(), NewPartitions.increaseTo(this.topic1.numPartitions())));
		}
	}

	@Configuration
	public static class Config {

		@Bean
		public EmbeddedKafkaBroker kafkaEmbedded() {
			return new EmbeddedKafkaBroker(3);
		}

		@Bean
		public KafkaAdmin admin() {
			Map<String, Object> configs = new HashMap<>();
			configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
					StringUtils.arrayToCommaDelimitedString(kafkaEmbedded().getBrokerAddresses()));
			return new KafkaAdmin(configs);
		}

		@Bean
		public NewTopic topic1() {
			return TopicBuilder.name("foo")
					.partitions(2)
					.replicas(1)
					.compact()
					.build();
		}

		@Bean
		public NewTopic topic2() {
			return TopicBuilder.name("bar")
					.replicasAssignments(Collections.singletonMap(0, Collections.singletonList(0)))
					.config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
					.build();
		}

		@Bean
		public NewTopic topic3() {
			return TopicBuilder.name("baz")
					.assignReplicas(0, Arrays.asList(0, 1))
					.assignReplicas(1, Arrays.asList(1, 2))
					.assignReplicas(2, Arrays.asList(2, 0))
					.config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
					.build();
		}

	}

}
