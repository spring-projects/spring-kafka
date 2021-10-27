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

package org.springframework.kafka.support;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.springframework.beans.factory.BeanCurrentlyInCreationException;
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

	private final Object beanOrClass;

	private final String methodName;

	private Object bean;

	private Method method;

	public EndpointHandlerMethod(Object beanOrClass, String methodName) {
		Assert.notNull(beanOrClass, () -> "No destination bean or class provided!");
		Assert.notNull(methodName, () -> "No method name for destination bean class provided!");
		this.beanOrClass = beanOrClass;
		this.methodName = methodName;
	}

	public EndpointHandlerMethod(Object bean, Method method) {
		Assert.notNull(bean, () -> "No bean for destination provided!");
		Assert.notNull(method, () -> "No method for destination bean class provided!");
		this.method = method;
		this.bean = bean;
		this.beanOrClass = bean.getClass();
		this.methodName = method.getName();
	}

	/**
	 * Return the method.
	 * @return the method.
	 */
	public Method getMethod() {
		if (this.beanOrClass instanceof Class) {
			return forClass((Class<?>) this.beanOrClass);
		}
		Assert.state(this.bean != null, "Bean must be resolved before accessing its method");
		if (this.bean instanceof EndpointHandlerMethod) {
			try {
				return Object.class.getMethod("toString");
			}
			catch (NoSuchMethodException | SecurityException e) {
			}
		}
		return forClass(this.bean.getClass());
	}

	/**
	 * Return the method name.
	 * @return the name.
	 * @since 2.8
	 */
	public String getMethodName() {
		Assert.state(this.methodName != null, "Unexpected call to getMethodName()");
		return this.methodName;
	}

	public Object resolveBean(BeanFactory beanFactory) {
		if (this.bean instanceof EndpointHandlerMethod) {
			return ((EndpointHandlerMethod) this.bean).beanOrClass;
		}
		if (this.bean == null) {
			try {
				if (this.beanOrClass instanceof Class) {
					Class<?> clazz = (Class<?>) this.beanOrClass;
					try {
						this.bean = beanFactory.getBean(clazz);
					}
					catch (NoSuchBeanDefinitionException e) {
						String beanName = clazz.getSimpleName() + "-handlerMethod";
						((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(beanName,
								new RootBeanDefinition(clazz));
						this.bean = beanFactory.getBean(beanName);
					}
				}
				else {
					String beanName = (String) this.beanOrClass;
					this.bean = beanFactory.getBean(beanName);
				}
			}
			catch (BeanCurrentlyInCreationException ex) {
				this.bean = this;
			}
		}
		return this.bean;
	}

	private Method forClass(Class<?> clazz) {
		if (this.method == null) {
			this.method = Arrays.stream(ReflectionUtils.getDeclaredMethods(clazz))
					.filter(mthd -> mthd.getName().equals(this.methodName))
					.findFirst()
					.orElseThrow(() -> new IllegalArgumentException(
							String.format("No method %s in class %s", this.methodName, clazz)));
		}
		return this.method;
	}

}
