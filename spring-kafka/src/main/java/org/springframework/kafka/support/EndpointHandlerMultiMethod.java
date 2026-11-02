/*
 * Copyright 2024-present the original author or authors.
 */

package org.springframework.kafka.support;

import java.lang.reflect.Method;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * Handler multi method for retrying endpoints.
 *
 * @author Wang Zhiyang
 *
 * @since 3.2
 *
 */
public class EndpointHandlerMultiMethod extends EndpointHandlerMethod {

	@Nullable
	private Method defaultMethod;

	private List<Method> methods;

	/**
	 * Construct an instance for the provided bean, defaultMethod and methods.
	 * @param bean the bean.
	 * @param defaultMethod the defaultMethod.
	 * @param methods the methods.
	 */
	public EndpointHandlerMultiMethod(Object bean, @Nullable Method defaultMethod, List<Method> methods) {
		super(bean);
		this.defaultMethod = defaultMethod;
		this.methods = methods;
	}

	/**
	 * Return the method list.
	 * @return the method list.
	 */
	public List<Method> getMethods() {
		return this.methods;
	}

	/**
	 * Set the method list.
	 * @param methods the method list.
	 */
	public void setMethods(List<Method> methods) {
		this.methods = methods;
	}

	/**
	 * Return the default method.
	 * @return the default method.
	 */
	public @Nullable Method getDefaultMethod() {
		return this.defaultMethod;
	}

	/**
	 * Set the default method.
	 * @param defaultMethod the default method.
	 */
	public void setDefaultMethod(Method defaultMethod) {
		this.defaultMethod = defaultMethod;
	}

}
