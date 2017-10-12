package com.kc14.hadoop.codec.binary;

public class GenerateHexToByteArray {

	public static void main(String[] args) {
		System.out.println("private final static byte[/* high digit */][/* low */] hex2byte = {");
		printHighLow();
		System.out.println("};");
	}

	private static void printHighLow() {
		for (int i = 0; i <= 0x0F; ++i) {
			printLow(i);
		}
	}

	public final static String FOUR_SPACES = "    ";
	
	private static void printLow(int i) {
		String indent = FOUR_SPACES + FOUR_SPACES;
		System.out.format("    {");
		for (int j = 0; j <= 0x0F; ++j) {
			if (j % 4 == 0) System.out.format("\n%s", indent);
			System.out.format("(byte) 0x%02x, ", i * 0x10 + j);
		}
		System.out.format("\n");
		System.out.println("    },");
	}

}
