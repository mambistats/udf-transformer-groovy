package com.kc14.hadoop.hive.transformers.udf.ipv6;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;

import com.kc14.janvanbesien.com.googlecode.ipv6.IPv6Address;
import com.kc14.janvanbesien.com.googlecode.ipv6.IPv6AddressRange;

public class DisjunctPartitioningMerger {
	
	// Using closed integer interval arithmetic!
	public static <E> void merge(final Iterator<IPv6AddressRange> A, final Iterator<IPv6AddressRange> B,
			                     final Iterator<Collection<E>> AP, final Iterator<Collection<E>> BP,
			                     final Collection<IPv6AddressRange> C, final Collection<Collection<E>> CP,
			                     Collection<E> prototype // Type Tagger
			                     ) {
        IPv6AddressRange a =  getNextRange(A);
		Collection<E>    ap = getNextProperties(AP);
		
		IPv6AddressRange b =  getNextRange(B);
		Collection<E>    bp = getNextProperties(BP);

		Collection<E> lhsNullElement = createNullElement(ap, prototype);
		
        // Process ranges
		while (a != null && b != null) {
			if (a.getLast().compareTo(b.getFirst()) < 0) {
				// Non-overlapping case 1
				// Interval A: |-----|
				// Interval B:         |-----|
				C.add(a);
				CP.add(addApAndNull(ap, prototype));
				a = getNextRange(A);
				ap = getNextProperties(AP);
			}
			else if (b.getLast().compareTo(a.getFirst()) < 0) {
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
				final int A_Left_CompareTo_B_Left = a.getFirst().compareTo(b.getFirst());
				final int A_Right_CompareTo_B_Right = a.getLast().compareTo(b.getLast());
				if (A_Left_CompareTo_B_Left < 0) {
					// Interval A: |--+-----....
					// Interval B:    |-------....
					C.add(IPv6AddressRange.fromFirstAndLast(a.getFirst(), predecessor(b.getFirst())));
					CP.add(addApAndNull(ap, prototype));
					if (A_Right_CompareTo_B_Right < 0) {
						// Interval A: |--+-----|
						// Interval B:    |-----+-----|
						C.add(IPv6AddressRange.fromFirstAndLast(b.getFirst(), a.getLast())); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						b = IPv6AddressRange.fromFirstAndLast(successor(a.getLast()), b.getLast());
						// bp = bp;
						a = getNextRange(A);
						ap = getNextProperties(AP);
					}
					else if (A_Right_CompareTo_B_Right > 0) {
						// Interval A: |--+------+-|
						// Interval B:    |------|
						C.add(b); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						a = IPv6AddressRange.fromFirstAndLast(successor(b.getLast()), a.getLast());
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
					C.add(IPv6AddressRange.fromFirstAndLast(b.getFirst(), predecessor(a.getFirst())));
					CP.add(addNullElementAndBp(lhsNullElement, bp, prototype));
					if (A_Right_CompareTo_B_Right > 0) {
						// Interval A:    |-----+-----|
						// Interval B: |--+-----|
						C.add(IPv6AddressRange.fromFirstAndLast(a.getFirst(), b.getLast())); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						a = IPv6AddressRange.fromFirstAndLast(successor(b.getLast()), a.getLast());
						// ap = ap
						b = getNextRange(B);
						bp = getNextProperties(BP);
					}
					else if (A_Right_CompareTo_B_Right < 0) {
						// Interval A:    |------|
						// Interval B: |--+------+-|
						C.add(a); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						b = IPv6AddressRange.fromFirstAndLast(successor(a.getLast()), b.getLast());
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
						b = IPv6AddressRange.fromFirstAndLast(successor(a.getLast()), b.getLast());
						// bp = bp;
						a = getNextRange(A);
						ap = getNextProperties(AP);
					}
					else if (A_Right_CompareTo_B_Right > 0) {
						// Interval A: |-----+-----|
						// Interval B: |-----|
						C.add(b); // A v B
						CP.add(addApAndBp(ap, bp, prototype)); // New interval gets properties of both intervals
						a = IPv6AddressRange.fromFirstAndLast(successor(b.getLast()), a.getLast());
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


	private static IPv6AddressRange getNextRange(Iterator<IPv6AddressRange> A) {
		return A.hasNext() ? A.next() : null;
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

	
	private static IPv6Address successor(final IPv6Address ipaddr) {
		return ipaddr.add(1); // ++addr
	}

	private static IPv6Address predecessor(final IPv6Address ipaddr) {
		return ipaddr.subtract(1); // --addr
	}
	
}
