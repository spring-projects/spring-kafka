/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * Handler method for retrying endpoints.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 */
public class EndpointHandlerMethod {

	private final Class<?> beanClass;

	private final Method method;

	private Object bean;

	public EndpointHandlerMethod(Class<?> beanClass, String methodName) {
		Assert.notNull(beanClass, () -> "No destination bean class provided!");
		Assert.notNull(methodName, () -> "No method name for destination bean class provided!");
		this.method = Arrays.stream(ReflectionUtils.getDeclaredMethods(beanClass))
				.filter(mthd -> mthd.getName().equals(methodName))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						String.format("No method %s in class %s", methodName, beanClass)));
		this.beanClass = beanClass;
	}

	public EndpointHandlerMethod(Object bean, Method method) {
		Assert.notNull(bean, () -> "No bean for destination provided!");
		Assert.notNull(method, () -> "No method for destination bean class provided!");
		this.method = method;
		this.bean = bean;
		this.beanClass = bean.getClass();
	}

	/**
	 * Return the method.
	 * @return the method.
	 */
	public Method getMethod() {
		return this.method;
	}

	public Object resolveBean(BeanFactory beanFactory) {
		if (this.bean == null) {
			try {
				this.bean = beanFactory.getBean(this.beanClass);
			}
			catch (NoSuchBeanDefinitionException e) {
				String beanName = this.beanClass.getSimpleName() + "-handlerMethod";
				((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(beanName,
						new RootBeanDefinition(this.beanClass));
				this.bean = beanFactory.getBean(beanName);
			}
		}
		return this.bean;
	}

}
