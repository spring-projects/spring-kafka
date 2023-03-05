package com.common;
public class Foo1 {
  private String foo;
  public Foo1() {}
  public Foo1(String foo) { this.foo = foo; }
  public String getFoo() { return this.foo; }
  @Override
  public String toString() {
    return "Foo1 [foo=]" + this.foo + "]";
  }
}