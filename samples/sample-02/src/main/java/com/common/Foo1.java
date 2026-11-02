/*
 * Copyright 2018-present the original author or authors.
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
