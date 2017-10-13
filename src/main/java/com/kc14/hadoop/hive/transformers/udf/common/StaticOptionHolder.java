package com.kc14.hadoop.hive.transformers.udf.common;

public class StaticOptionHolder {
	
	private static final String DEFAULT_INPUT_SEP = "\t";
	private static final String DEFAULT_OUTPUT_SEP = "\t";
	private static final String DEFAULT_ARRAY_SEP = "\u0002"; // I.e. ASCII STX (Start of Text: x02)
	private static final String DEFAULT_HIVE_NULL_STR = "\\N";
	

	public static String inputsep = DEFAULT_INPUT_SEP;
	public static String outputsep = DEFAULT_OUTPUT_SEP;
	public static String arraysep = DEFAULT_ARRAY_SEP;
	public static String hivenull = DEFAULT_HIVE_NULL_STR;

}
