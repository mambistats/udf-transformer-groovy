package com.kc14.algebra;

public interface CountableInterval<E extends Countable<E>> {
	E first();
	E last();
	CountableInterval<E> newFromFirstAndLast(E first, E last);
}
