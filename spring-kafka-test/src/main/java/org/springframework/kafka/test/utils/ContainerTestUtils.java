/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.test.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodCallback;
import org.springframework.util.ReflectionUtils.MethodFilter;

/**
 * Utilities for testing listener containers. No hard references to container
 * classes are used to avoid circular project dependencies.
 *
 * @author Gary Russell
 * @since 1.0.3
 */
public final class ContainerTestUtils {

	private ContainerTestUtils() {
		// private ctor
	}

	/**
	 * Wait until the container has the required number of assigned partitions.
	 * @param container the container.
	 * @param partitions the number of partitions.
	 * @throws Exception an exception.
	 */
	public static void waitForAssignment(Object container, int partitions)
			throws Exception {
		if (container.getClass().getSimpleName().contains("KafkaMessageListenerContainer")) {
			waitForSingleContainerAssignment(container, partitions);
			return;
		}
		List<?> containers = KafkaTestUtils.getPropertyValue(container, "containers", List.class);
		int n = 0;
		int count = 0;
		Method getAssignedPartitions = null;
		while (n++ < 600 && count < partitions) {
			count = 0;
			for (Object aContainer : containers) {
				if (getAssignedPartitions == null) {
					getAssignedPartitions = getAssignedPartitionsMethod(aContainer.getClass());
				}
				Collection<?> assignedPartitions = (Collection<?>) getAssignedPartitions.invoke(aContainer);
				if (assignedPartitions != null) {
					count += assignedPartitions.size();
				}
			}
			if (count < partitions) {
				Thread.sleep(100);
			}
		}
		assertThat(count).isEqualTo(partitions);
	}

	private static void waitForSingleContainerAssignment(Object container, int partitions)
			throws Exception {
		int n = 0;
		int count = 0;
		Method getAssignedPartitions = getAssignedPartitionsMethod(container.getClass());
		while (n++ < 600 && count < partitions) {
			count = 0;
			Collection<?> assignedPartitions = (Collection<?>) getAssignedPartitions.invoke(container);
			if (assignedPartitions != null) {
				count = assignedPartitions.size();
			}
			if (count < partitions) {
				Thread.sleep(100);
			}
		}
		assertThat(count).isEqualTo(partitions);
	}

	private static Method getAssignedPartitionsMethod(Class<?> clazz) {
		final AtomicReference<Method> theMethod = new AtomicReference<Method>();
		ReflectionUtils.doWithMethods(clazz, new MethodCallback() {

			@Override
			public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
				theMethod.set(method);
			}
		}, new MethodFilter() {

			@Override
			public boolean matches(Method method) {
				return method.getName().equals("getAssignedPartitions") && method.getParameterTypes().length == 0;
			}
		});
		assertThat(theMethod.get()).isNotNull();
		return theMethod.get();
	}

}
