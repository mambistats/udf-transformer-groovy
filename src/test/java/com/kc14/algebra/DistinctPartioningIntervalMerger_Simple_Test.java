package com.kc14.algebra;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import com.kc14.janvanbesien.com.googlecode.ipv6.IPv6Address;
import com.kc14.janvanbesien.com.googlecode.ipv6.IPv6AddressRange;


public class DistinctPartioningIntervalMerger_Simple_Test {

	@Test
	public void mergeShouldReturnThreeRanges() {
		IPv6AddressRange[] A = new IPv6AddressRange[] { IPv6AddressRange.fromFirstAndLast(IPv6Address.fromLongs(0, 1), IPv6Address.fromLongs(0, 15)) };
		IPv6AddressRange[] B = new IPv6AddressRange[] { IPv6AddressRange.fromFirstAndLast(IPv6Address.fromLongs(0, 5), IPv6Address.fromLongs(0, 8)) };
		
		List<Collection<String>> AP = new ArrayList<>();
		AP.add(Arrays.asList("1to15"));
		
		List<Collection<String>> BP = new ArrayList<>();
		BP.add(Arrays.asList("5to8"));
		
		// Maximum Size = 2 (m + n) ?
		List<IPv6AddressRange>   C = new ArrayList<>();
		List<Collection<String>> CP = new ArrayList<>();
		ArrayList<String> prototype = new ArrayList<>(0); // Type Tagger
		
		DisjunctPartitioningIntervalMerger.merge(Arrays.asList(A).iterator(), Arrays.asList(B).iterator(), AP.iterator(), BP.iterator(), C, CP, prototype);
		
		for (int i = 0; i < C.size(); ++i) {
			System.out.format("[%d]: [%s] => %s", i, C.get(i), CP.get(i));
			System.out.println();
		}
		System.out.println();
		
		String C_actual = Arrays.toString(C.toArray());
		String CP_actual = Arrays.toString(CP.toArray());
		
		String C_expected = "[::1 - ::4, ::5 - ::8, ::9 - ::f]";
		String CP_expected = "[[1to15, null], [1to15, 5to8], [1to15, null]]";

		assertEquals("Correct Output for merged Ranges", C_expected, C_actual);
		assertEquals("Correct Output for merged Properties ", CP_expected, CP_actual);
	}

}
