/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package com.common;

/**
 * @author Gary Russell
 * @since 5.1
 *
 */
public class Foo2 {

	private String foo;

	public Foo2() {
	}

	public Foo2(String foo) {
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
		return "Foo2 [foo=" + this.foo + "]";
	}

}
