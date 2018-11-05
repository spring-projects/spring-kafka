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

package com.common;

/**
 * @author Gary Russell
 * @since 5.1
 *
 */
public class Foo1 {

	private String foo;

	public Foo1() {
		super();
	}

	public Foo1(String foo) {
		this.foo = foo;
	}

	public String getFoo() {
		return this.foo;
	}

	public void setFoo(String foo) {
		this.foo = foo;
	}

	@Override
	public String toString() {
		return "Foo1 [foo=" + this.foo + "]";
	}

}
