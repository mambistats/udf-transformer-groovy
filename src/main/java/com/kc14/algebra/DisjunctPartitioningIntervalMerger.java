package com.kc14.algebra;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;

public class DisjunctPartitioningIntervalMerger {
	
	// Using closed integer interval arithmetic!
	public static <T extends Countable<T>, I extends CountableInterval<T>, P>
	void merge(
			final Iterator<I> A, final Iterator<I> B,
			final Iterator<Collection<P>> AP, final Iterator<Collection<P>> BP,
			final Collection<I> C, final Collection<Collection<P>> CP,
			final Collection<P> prototype // Type Tagger
			) {
        I a =  getNextRange(A);
		Collection<P>    ap = getNextProperties(AP);
		
		I b =  getNextRange(B);
		Collection<P>    bp = getNextProperties(BP);
		
		I f = a != null ? a : b; // Factory for new intervals

		Collection<P> lhsNullElement = createNullElement(ap, prototype);
		
        // Process ranges
		while (a != null && b != null) {
			if (a.last().compareTo(b.first()) < 0) {
				// Non-overlapping case 1
				// Interval A: |-----|
				// Interval B:         |-----|
				C.add(a);
				CP.add(addApAndNull(ap, prototype));
				a = getNextRange(A);
				ap = getNextProperties(AP);
			}
			else if (b.last().compareTo(a.first()) < 0) {
				// Non-overlapping case 2
				// Interval A:         |-----|
				// Interval B: |-----|
				C.add(b);
				CP.add(addNullElementAndBp(lhsNullElement, bp, prototype));
				b = getNextRange(B);
				bp = getNextProperties(BP);
			}
			else {
				// Intersection between A and B
				final int A_Left_CompareTo_B_Left = a.first().compareTo(b.first());
				final int A_Right_CompareTo_B_Right = a.last().compareTo(b.last());
				if (A_Left_CompareTo_B_Left < 0) {
					// Interval A: |--+-----....
					// Interval B:    |-------....
					C.add((I) f.newFromFirstAndLast(a.first(), b.first().predecessor()));
					CP.add(addApAndNull(ap, prototype));
					if (A_Right_CompareTo_B_Right < 0) {
						// Interval A: |--+-----|
						// Interval B:    |-----+-----|
						C.add((I) f.newFromFirstAndLast(b.first(), a.last())); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						b = (I) f.newFromFirstAndLast(a.last().successor(), b.last());
						// bp = bp;
						a = getNextRange(A);
						ap = getNextProperties(AP);
					}
					else if (A_Right_CompareTo_B_Right > 0) {
						// Interval A: |--+------+-|
						// Interval B:    |------|
						C.add(b); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						a = (I) f.newFromFirstAndLast(b.last().successor(), a.last());
						// ap = ap;
						b = getNextRange(B);
						bp = getNextProperties(BP);
					}
					else {
						// Interval A: |--+--------|
						// Interval B:    |--------|
						C.add(b); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						a = getNextRange(A);
						ap = getNextProperties(AP);
						b = getNextRange(B);
						bp = getNextProperties(BP);
					}
				}
				else if (A_Left_CompareTo_B_Left > 0) {
					// Interval A:    |-------....
					// Interval B: |--+-----....
					C.add((I) f.newFromFirstAndLast(b.first(), a.first().predecessor()));
					CP.add(addNullElementAndBp(lhsNullElement, bp, prototype));
					if (A_Right_CompareTo_B_Right > 0) {
						// Interval A:    |-----+-----|
						// Interval B: |--+-----|
						C.add((I) f.newFromFirstAndLast(a.first(), b.last())); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						a = (I) f.newFromFirstAndLast(b.last().successor(), a.last());
						// ap = ap
						b = getNextRange(B);
						bp = getNextProperties(BP);
					}
					else if (A_Right_CompareTo_B_Right < 0) {
						// Interval A:    |------|
						// Interval B: |--+------+-|
						C.add(a); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						b = (I) f.newFromFirstAndLast(a.last().successor(), b.last());
						// bp = bp;
						a = getNextRange(A);
						ap = getNextProperties(AP);
					}
					else {
						// Interval A:    |--------|
						// Interval B: |--+--------|
						C.add(a); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						a = getNextRange(A);
						ap = getNextProperties(AP);
						b = getNextRange(B);
						bp = getNextProperties(BP);
					}
				}
				else {
					// Interval A: |-------....
					// Interval B: |-------....
					if (A_Right_CompareTo_B_Right < 0) {
						// Interval A: |-----|
						// Interval B: |-----+-----|
						C.add(a); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						b = (I) f.newFromFirstAndLast(a.last().successor(), b.last());
						// bp = bp;
						a = getNextRange(A);
						ap = getNextProperties(AP);
					}
					else if (A_Right_CompareTo_B_Right > 0) {
						// Interval A: |-----+-----|
						// Interval B: |-----|
						C.add(b); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						a = (I) f.newFromFirstAndLast(b.last().successor(), a.last());
						// ap = ap;
						b = getNextRange(B);
						bp = getNextProperties(BP);
					}
					else {
						// Interval A: |--------|
						// Interval B: |--------|
						C.add(a); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						a = getNextRange(A);
						ap = getNextProperties(AP);
						b = getNextRange(B);
						bp = getNextProperties(BP);
					}
				}
			}			
		}		
		if (a != null) { // Boundary solution: B completely processed or empty, but right boundary of current interval in A not!
			// Append interval in progress
			C.add(a);
			CP.add(addApAndNull(ap, prototype));
			// Append rest of A
			while (A.hasNext()) C.add(A.next());
			while (AP.hasNext()) CP.add(addApAndNull(AP.next(), prototype));
		}
		else if (b != null) { // Boundary solution: A completely processed or empty, but right boundary of current interval in B not!
			// Append interval in progress
			C.add(b);
			CP.add(addNullElementAndBp(lhsNullElement,bp, prototype));
			// Append rest of B
			while (B.hasNext()) C.add(B.next());
			while (BP.hasNext()) CP.add(addNullElementAndBp(lhsNullElement, BP.next(), prototype));
		}
		return;
	}


	private static <E> Collection<E> createNullElement(Collection<E> ap, Collection<E> prototype) {
		// The length of the first element in AP defines the length of the null element for AP
        final int nullElementLength = ap != null ? ap.size() : 0;
        Collection<E> nullElement = cloneByType(prototype, nullElementLength);
        for (int i = 0; i < nullElementLength; ++i) {
        	nullElement.add(null);
        }
		return nullElement;
	}


	private static <I> I getNextRange(Iterator<I> a) {
		return a.hasNext() ? a.next() : null;
	}

	private static <E> Collection<E> getNextProperties(Iterator<Collection<E>> P) {
		return P.hasNext() ? P.next() : null;
	}
	
	
	private static <E> Collection<E> addApAndNull(Collection<E> ap, Collection<E> prototype) {
		Collection<E> cp = cloneByType(prototype, ap.size() + 1);
		cp.addAll(ap);
		cp.add(null);
		return cp;
	}

	private static <E> Collection<E> addNullElementAndBp(Collection<E> nullElement, Collection<E> collection, Collection<E> prototype) {
		Collection<E> cp = cloneByType(prototype, nullElement.size() + collection.size());
		cp.addAll(nullElement);
		cp.addAll(collection);
		return cp;
	}

	private static <E> Collection<E> addApAndBp(Collection<E> ap, Collection<E> bp, Collection<E> prototype) {
		Collection<E> cp = cloneByType(prototype, ap.size() + bp.size());
		cp.addAll(ap);
		cp.addAll(bp);
		return cp;
	}

	private static <E> Collection<E> cloneByType (Collection<E> collection, int capacity) {
		Collection<E> newInstance = null;
		try {
			newInstance = collection.getClass().getConstructor(Integer.TYPE).newInstance(capacity);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			// Choose a collection with a capacity constructor
			e.printStackTrace();
		}
		return newInstance;
	}
	
}
