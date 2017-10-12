package com.kc14.hadoop.codec.binary;

public class GenerateByteToHexArray {
	
	private static char[] nibbleToDigit = {
		'0', '1', '2', '3', '4', '5', '6', '7',
		'8', '9', 'A', 'B', 'C', 'D', 'E', 'F',	
	};

	public static void main(String[] args) {
		System.out.format("private final static String[/* byte */] byte2hex = {");
		printHexPairs();
		System.out.format("\n};");
		System.out.println();
	}

	public final static String FOUR_SPACES = "    ";
	
	private static void printHexPairs() {
		String indent = FOUR_SPACES;
		for (int i = 0x00; i <= 0xFF; ++i) {
			if (i % 8 == 0) { System.out.format("\n%s", indent); }
			int lowNibble = i & 0x0F;
			int highNibble = (i >> 4) & 0x0F;
			System.out.format("\"%c%c\", ", nibbleToDigit[highNibble], nibbleToDigit[lowNibble]);
		}
	}

}
