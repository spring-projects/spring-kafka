/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StringUtils;

/**
 * @author Gary Russell
 * @since 1.3
 *
 */
@RunWith(SpringRunner.class)
@DirtiesContext
public class KafkaAdminTests {

	@Autowired
	private KafkaAdmin admin;

	@Autowired
	private NewTopic topic1;

	@Autowired
	private NewTopic topic2;

	@Test
	public void testAddTopics() throws Exception {
		AdminClient adminClient = AdminClient.create(this.admin.getConfig());
		DescribeTopicsResult topics = adminClient.describeTopics(Arrays.asList("foo", "bar"));
		topics.all().get();
		new DirectFieldAccessor(this.topic1).setPropertyValue("numPartitions", 2);
		new DirectFieldAccessor(this.topic2).setPropertyValue("numPartitions", 3);
		this.admin.initialize();
		topics = adminClient.describeTopics(Arrays.asList("foo", "bar"));
		Map<String, TopicDescription> results = topics.all().get();
		results.forEach((name, td) -> assertThat(td.partitions()).hasSize(name.equals("foo") ? 2 : 3));
		adminClient.close(10, TimeUnit.SECONDS);
	}

	@Configuration
	public static class Config {

		@Bean
		public EmbeddedKafkaBroker kafkaEmbedded() {
			return new EmbeddedKafkaBroker(1);
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
			return new NewTopic("foo", 1, (short) 1);
		}

		@Bean
		public NewTopic topic2() {
			return new NewTopic("bar", 1, (short) 1);
		}

	}

}
