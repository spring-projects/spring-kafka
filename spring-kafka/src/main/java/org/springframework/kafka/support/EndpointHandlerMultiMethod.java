/*
 * Copyright 2024 the original author or authors.
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
import java.util.List;

/**
 * Handler multi method for retrying endpoints.
 *
 * @author Wang Zhiyang
 *
 * @since 3.2
 *
 */
public class EndpointHandlerMultiMethod extends EndpointHandlerMethod {

	private Method defaultMethod;

	private List<Method> methods;

	public EndpointHandlerMultiMethod(Object bean, Method defaultMethod, List<Method> methods) {
		super(bean);
		this.defaultMethod = defaultMethod;
		this.methods = methods;
	}


	public List<Method> getMethods() {
		return this.methods;
	}

	public void setMethods(List<Method> methods) {
		this.methods = methods;
	}

	public Method getDefaultMethod() {
		return this.defaultMethod;
	}

	public void setDefaultMethod(Method defaultMethod) {
		this.defaultMethod = defaultMethod;
	}

}
