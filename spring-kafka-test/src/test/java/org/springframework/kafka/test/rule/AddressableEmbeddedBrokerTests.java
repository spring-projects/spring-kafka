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

package org.springframework.kafka.test.rule;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;

import javax.net.ServerSocketFactory;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Gary Russell
 * @author Kamill Sokol
 * @author Elliot Kennedy
 * @since 1.3
 *
 */
@RunWith(SpringRunner.class)
public class AddressableEmbeddedBrokerTests {

	@Autowired
	private Config config;

	@Autowired
	private KafkaEmbedded broker;

	@Test
	public void testKafkaEmbedded() {
		assertThat(broker.getBrokersAsString()).isEqualTo("127.0.0.1:" + this.config.port);
		assertThat(broker.getBrokersAsString())
				.isEqualTo(System.getProperty(KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS));
		assertThat(broker.getZookeeperConnectionString())
				.isEqualTo(System.getProperty(KafkaEmbedded.SPRING_EMBEDDED_ZOOKEEPER_CONNECT));
	}

	@Configuration
	public static class Config {

		private int port;

		@Bean
		public KafkaEmbedded broker() throws IOException {
			KafkaEmbedded broker = new KafkaEmbedded(1);
			ServerSocket ss = ServerSocketFactory.getDefault().createServerSocket(0);
			this.port = ss.getLocalPort();
			ss.close();
			broker.setKafkaPorts(this.port);
			return broker;
		}

	}

}
