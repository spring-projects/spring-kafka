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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

/**
 * @author Gary Russell
 *
 */
public final class ContainerTestUtils {

	private ContainerTestUtils() {
		// private ctor
	}

	public static void waitForAssignment(ConcurrentMessageListenerContainer<Integer, String> container, int partitions)
			throws Exception {
		List<KafkaMessageListenerContainer<Integer, String>> containers = container.getContainers();
		int n = 0;
		int count = 0;
		while (n++ < 600 && count < partitions) {
			count = 0;
			for (KafkaMessageListenerContainer<Integer, String> aContainer : containers) {
				if (aContainer.getAssignedPartitions() != null) {
					count += aContainer.getAssignedPartitions().size();
				}
			}
			if (count < partitions) {
				Thread.sleep(100);
			}
		}
		assertThat(count).isEqualTo(partitions);
	}

	public static void waitForAssignment(KafkaMessageListenerContainer<Integer, String> container, int partitions)
			throws Exception {
		int n = 0;
		int count = 0;
		while (n++ < 600 && count < partitions) {
			count = 0;
			if (container.getAssignedPartitions() != null) {
				count = container.getAssignedPartitions().size();
			}
			if (count < partitions) {
				Thread.sleep(100);
			}
		}
		assertThat(count).isEqualTo(partitions);
	}

}
