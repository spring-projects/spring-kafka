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

package org.springframework.kafka.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

/**
 * @author Soby Chacko
 */
public class StreamsBuilderFactoryLateConfigTests {

	private static final String APPLICATION_ID = "streamsBuilderFactoryLateConfigTests";

	@Test(expected = IllegalArgumentException.class)
	public void testStreamBuilderFactoryCannotBeStartedWithoutStreamconfig() throws Exception {
		StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean();
		streamsBuilderFactoryBean.createInstance();
	}

	@Test
	public void testStreamsBuilderFactoryWithConfigProvidedLater() throws Exception {
		StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean();

		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost");
		StreamsConfig streamsConfig = new StreamsConfig(props);
		streamsBuilderFactoryBean.setStreamsConfig(streamsConfig);

		StreamsBuilder streamsBuilder = streamsBuilderFactoryBean.createInstance();
		assertThat(streamsBuilder).isNotNull();
	}
}
