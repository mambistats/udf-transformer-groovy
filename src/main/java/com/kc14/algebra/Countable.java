package com.kc14.algebra;

public interface Countable<T> extends Comparable<T> {
	T predecessor();
	T successor();
}
