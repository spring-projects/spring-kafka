/*
 * Copyright 2017-present the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Adrian Gygax
 * @author Anders Swanson
 *
 * @since 1.3
 */
@SpringJUnitConfig
@DirtiesContext
public class KafkaAdminTests {

	@Autowired
	private KafkaAdmin admin;

	@Autowired
	private AdminClient adminClient;

	@Autowired
	private NewTopic topic1;

	@Autowired
	private NewTopic topic2;

	@Autowired
	private NewTopic topic3;

	@Autowired
	private NewTopic mismatchconfig;

	@Autowired
	private NewTopic dontCreateThisOne;

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
		assertThat(admin.newTopics()).doesNotContain(this.dontCreateThisOne);
	}

	@Test
	@Disabled
	public void testAddTopicsAndAddPartitions() throws Exception {
		Map<String, TopicDescription> results = this.admin.describeTopics("foo", "bar");
		results.forEach((name, td) -> assertThat(td.partitions()).hasSize(name.equals("foo") ? 2 : 1));
		new DirectFieldAccessor(this.topic1).setPropertyValue("numPartitions", Optional.of(4));
		new DirectFieldAccessor(this.topic2).setPropertyValue("numPartitions", Optional.of(3));
		this.admin.initialize();
		this.admin.setModifyTopicConfigs(true);

		int n = 0;
		await().until(() -> {
			results.putAll(this.admin.describeTopics("foo", "bar"));
			TopicDescription foo = results.values().stream()
					.filter(tp -> tp.name().equals("foo"))
					.findFirst()
					.get();
			TopicDescription bar = results.values().stream()
					.filter(tp -> tp.name().equals("bar"))
					.findFirst()
					.get();
			return foo.partitions().size() == 4 && bar.partitions().size() == 3;
		});
		results.forEach((name, td) -> assertThat(td.partitions()).hasSize(name.equals("foo") ? 4 : 3));
		new DirectFieldAccessor(this.topic1).setPropertyValue("numPartitions", Optional.of(5));
		this.admin.createOrModifyTopics(this.topic1,
				TopicBuilder.name("qux")
					.partitions(5)
					.build());
		results.clear();
		await().until(() -> {
			results.putAll(this.admin.describeTopics("foo", "qux"));
			TopicDescription foo = results.values().stream()
					.filter(tp -> tp.name().equals("foo"))
					.findFirst()
					.get();
			return foo.partitions().size() == 5;
		});
		results.forEach((name, td) -> assertThat(td.partitions()).hasSize(5));

		await().until(() -> {
			adminClient.incrementalAlterConfigs(
					Map.of(
							new ConfigResource(Type.TOPIC, "mismatchconfig"),
							List.of(new AlterConfigOp(new ConfigEntry("retention.bytes", "10"), OpType.SET),
									new AlterConfigOp(new ConfigEntry("retention.ms", "11"), OpType.SET))));
			DescribeConfigsResult describeConfigsResult = this.adminClient
					.describeConfigs(List.of(new ConfigResource(Type.TOPIC, "mismatchconfig")));
			Map<ConfigResource, org.apache.kafka.clients.admin.Config> configResourceConfigMap = describeConfigsResult.all()
					.get();
			return configResourceConfigMap.get(new ConfigResource(Type.TOPIC, "mismatchconfig")).get("retention.bytes").value().equals("10")
					&& configResourceConfigMap.get(new ConfigResource(Type.TOPIC, "mismatchconfig")).get("retention.ms").value().equals("11");
		});

		this.admin.createOrModifyTopics(mismatchconfig,
				TopicBuilder.name("noConfigAddLater")
						.partitions(2)
						.replicas(1)
						.config("retention.ms", "1000")
						.build());

		await().until(() -> {
			DescribeConfigsResult describeConfigsResult = this.adminClient
					.describeConfigs(List.of(new ConfigResource(Type.TOPIC, "mismatchconfig"),
							new ConfigResource(Type.TOPIC, "noConfigAddLater")));
			Map<ConfigResource, org.apache.kafka.clients.admin.Config> configResourceConfigMap = describeConfigsResult.all()
					.get();
			return configResourceConfigMap.get(new ConfigResource(Type.TOPIC, "mismatchconfig"))
					.get("retention.bytes").value().equals("1024")
					&& configResourceConfigMap.get(new ConfigResource(Type.TOPIC, "mismatchconfig"))
					.get("retention.ms").value().equals("1111")
					&& configResourceConfigMap.get(new ConfigResource(Type.TOPIC, "noConfigAddLater"))
					.get("retention.ms").value().equals("1000");
		});

		assertThatIllegalStateException().isThrownBy(() -> this.admin.createOrModifyTopics(mismatchconfig,
				TopicBuilder.name("noConfigAddLater")
						.partitions(2)
						.replicas(1)
						.config("no.such.config.key", "1000")
						.build()))
				.withMessageContaining("no.such.config.key");

	}

	@Test
	public void testDefaultPartsAndReplicas() throws Exception {
		try (AdminClient adminClient = AdminClient.create(this.admin.getConfigurationProperties())) {
			Map<String, TopicDescription> results = new HashMap<>();
			await().atMost(10, TimeUnit.SECONDS).until(() -> {
				try {
					DescribeTopicsResult topics = adminClient.describeTopics(Arrays.asList("optBoth", "optPart", "optRepl"));

					// Use CompletableFuture to handle the async operation
					CompletableFuture<Map<String, TopicDescription>> future =
							topics.allTopicNames().toCompletionStage().toCompletableFuture();

					try {
						Map<String, TopicDescription> topicNames = future.get(5, TimeUnit.SECONDS);
						results.putAll(topicNames);
						return true;
					}
					catch (ExecutionException ex) {
						if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
							// Topics don't exist yet, so create them with correct replication factor
							return false;
						}
						throw ex;
					}
				}
				catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					return true;
				}
				catch (TimeoutException te) {
					// Timeout getting the future, try again
					return false;
				}
			});

			var topicDescription = results.get("optBoth");
			assertThat(topicDescription.partitions()).hasSize(1);
			assertThat(topicDescription.partitions().stream()
					.map(tpi -> tpi.replicas())
					.flatMap(nodes -> nodes.stream())
					.count()).isEqualTo(1);
			topicDescription = results.get("optPart");
			assertThat(topicDescription.partitions()).hasSize(1);
			assertThat(topicDescription.partitions().stream()
					.map(tpi -> tpi.replicas())
					.flatMap(nodes -> nodes.stream())
					.count()).isEqualTo(1);
			topicDescription = results.get("optRepl");
			assertThat(topicDescription.partitions()).hasSize(3);
			assertThat(topicDescription.partitions().stream()
					.map(tpi -> tpi.replicas())
					.flatMap(nodes -> nodes.stream())
					.count()).isEqualTo(3);
		}
	}

	@Test
	public void alreadyExists() throws Exception {
		AtomicReference<Method> addTopics = new AtomicReference<>();
		AtomicReference<Method> modifyTopics = new AtomicReference<>();
		ReflectionUtils.doWithMethods(KafkaAdmin.class, m -> {
			if (m.getName().equals("addTopics")) {
				m.setAccessible(true);
				addTopics.set(m);
			}
			else if (m.getName().equals("createMissingPartitions")) {
				m.setAccessible(true);
				modifyTopics.set(m);
			}
		});
		try (AdminClient adminClient = AdminClient.create(this.admin.getConfigurationProperties())) {
			addTopics.get().invoke(this.admin, adminClient, Collections.singletonList(this.topic1));
			modifyTopics.get().invoke(this.admin, adminClient, Collections.singletonMap(
					this.topic1.name(), NewPartitions.increaseTo(this.topic1.numPartitions())));
		}
	}

	@Test
	void toggleBootstraps() {
		Map<String, Object> config = new HashMap<>();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "foo");
		KafkaAdmin admin = new KafkaAdmin(config);
		ABSwitchCluster bootstrapServersSupplier = new ABSwitchCluster("a,b,c", "d,e,f");
		admin.setBootstrapServersSupplier(bootstrapServersSupplier);
		assertThat(admin.getConfigurationProperties().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG))
				.isEqualTo("a,b,c");
		bootstrapServersSupplier.secondary();
		assertThat(admin.getConfigurationProperties().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG))
				.isEqualTo("d,e,f");
		bootstrapServersSupplier.primary();
		assertThat(admin.getConfigurationProperties().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG))
				.isEqualTo("a,b,c");
	}

	@Test
	void nullClusterId() {
		AdminClient mock = mock(AdminClient.class);
		DescribeClusterResult result = mock(DescribeClusterResult.class);
		KafkaFuture<String> fut = KafkaFuture.completedFuture(null);
		given(result.clusterId()).willReturn(fut);
		given(mock.describeCluster()).willReturn(result);
		KafkaAdmin admin = new KafkaAdmin(Map.of()) {

			@Override
			protected Admin createAdmin() {
				return mock;
			}

		};
		assertThat(admin.clusterId()).isEqualTo("null");
	}

	@Test
	void getAdminConfigWithNoClientId() {
		KafkaAdmin kafkaAdmin = new KafkaAdmin(Map.of());
		assertThat(kafkaAdmin.getAdminConfig()).isEmpty();
	}

	@Test
	void getAdminConfigWithExplicitClientId() {
		Map<String, Object> config = Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "admin");
		KafkaAdmin kafkaAdmin = new KafkaAdmin(config);
		assertThat(kafkaAdmin.getAdminConfig()).containsExactlyInAnyOrderEntriesOf(config);
	}

	@Test
	void getAdminConfigWithApplicationNameAsClientId() {
		Map<String, Object> config = Map.of();
		KafkaAdmin kafkaAdmin = new KafkaAdmin(config);
		final Environment environment = mock(Environment.class);
		given(environment.getProperty("spring.application.name")).willReturn("appname");
		final ApplicationContext applicationContext = mock(ApplicationContext.class);
		given(applicationContext.getEnvironment()).willReturn(environment);
		kafkaAdmin.setApplicationContext(applicationContext);
		assertThat(kafkaAdmin.getAdminConfig()).containsOnly(Map.entry(AdminClientConfig.CLIENT_ID_CONFIG, "appname-admin-0"));
	}

	@Test
	void testDeleteTopics() {
		NewTopic testTopic1 = TopicBuilder.name("test-delete-1")
				.partitions(1)
				.replicas(1)
				.build();
		NewTopic testTopic2 = TopicBuilder.name("test-delete-2")
				.partitions(1)
				.replicas(1)
				.build();

		this.admin.createOrModifyTopics(testTopic1, testTopic2);

		await().atMost(10, TimeUnit.SECONDS).until(() -> {
			try {
				Map<String, TopicDescription> topics =
						this.admin.describeTopics("test-delete-1", "test-delete-2");
				return topics.size() == 2;
			}
			catch (Exception e) {
				return false;
			}
		});

		Map<String, TopicDescription> beforeDelete = this.admin.describeTopics("test-delete-1", "test-delete-2");
		assertThat(beforeDelete).hasSize(2);
		assertThat(beforeDelete).containsKeys("test-delete-1", "test-delete-2");

		this.admin.deleteTopics("test-delete-1", "test-delete-2");

		await().atMost(10, TimeUnit.SECONDS).until(() -> {
			try (AdminClient adminClient = AdminClient.create(this.admin.getConfigurationProperties())) {
				DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList("test-delete-1", "test-delete-2"));
				try {
					result.allTopicNames().get(5, TimeUnit.SECONDS);
					return false;
				}
				catch (ExecutionException ex) {
					return ex.getCause() instanceof UnknownTopicOrPartitionException;
				}
			}
			catch (InterruptedException | TimeoutException e) {
				return false;
			}
		});
	}

	@Test
	void testDeleteNonExistentTopic() {
		assertThat(org.assertj.core.api.Assertions.catchThrowable(() ->
				this.admin.deleteTopics("non-existent-topic-12345")
		)).isInstanceOf(org.springframework.kafka.KafkaException.class);
	}

	@Test
	void testDeleteTopicsWithEmptyArray() {
		this.admin.deleteTopics();
	}

	@Configuration
	public static class Config {

		@Bean
		public EmbeddedKafkaBroker kafkaEmbedded() {
			return new EmbeddedKafkaKraftBroker(1, 1)
					.brokerProperty("default.replication.factor", 2);
		}

		@Bean
		public KafkaAdmin admin() {
			Map<String, Object> configs = new HashMap<>();
			KafkaAdmin admin = new KafkaAdmin(configs);
			admin.setBootstrapServersSupplier(() -> kafkaEmbedded().getBrokersAsString());
			admin.setCreateOrModifyTopic(nt -> !nt.name().equals("dontCreate"));
			return admin;
		}

		@Bean
		public AdminClient adminClient() {
			Map<String, Object> configs = new HashMap<>();
			configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaEmbedded().getBrokersAsString());
			return AdminClient.create(configs);
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

		@Bean
		public NewTopic mismatchconfig() {
			return TopicBuilder.name("mismatchconfig")
					.partitions(2)
					.replicas(1)
					.config("retention.bytes", "1024")
					.config("retention.ms", "1111")
					.build();
		}

		@Bean
		public NewTopic noConfigAddLater() {
			return TopicBuilder.name("noConfigAddLater")
					.partitions(2)
					.replicas(1)
					.build();
		}

		@Bean
		public NewTopics topics456() {
			return new NewTopics(
					TopicBuilder.name("optBoth")
							.replicas(1)  // Explicitly set to 1 replica
							.build(),
					TopicBuilder.name("optPart")
							.replicas(1)  // Already correct
							.build(),
					TopicBuilder.name("optRepl")
							.partitions(3)
							.replicas(1)  // Explicitly set to 1 replica
							.build());
		}

		@Bean
		NewTopic dontCreateThisOne() {
			return TopicBuilder.name("dontCreate").build();
		}

	}

}
