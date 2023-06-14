/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.kafka.test;

import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
 * @since 3.1
 *
 */
public class EmbeddedKafkaKraftBrokerTests {

	@Test
	void testUpDown() {
		LogFactory.getLog(getClass()).info("foo");
		EmbeddedKafkaKraftBroker kafka = new EmbeddedKafkaKraftBroker(1, 1, "topic1");
		kafka.afterPropertiesSet();
		kafka.start();
		kafka.stop();
	}

}
