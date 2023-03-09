package com.common;
public class Foo2 {
  private String foo;
  public Foo2() {}
  public Foo2(String foo) { this.foo = foo; }
  public String getFoo() { return this.foo; }
  @Override
  public String toString() {
    return "Foo2 [foo=" + this.foo + "]";
  }
}