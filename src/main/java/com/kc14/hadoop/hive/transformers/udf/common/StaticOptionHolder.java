package com.kc14.hadoop.hive.transformers.udf.common;

public class StaticOptionHolder {
	
	private static final String DEFAULT_INPUT_SEP = "\t";
	private static final String DEFAULT_OUTPUT_SEP = "\t";
	private static final String DEFAULT_ARRAY_SEP = "\2";
	
	public static String inputsep = DEFAULT_INPUT_SEP;
	public static String outputsep = DEFAULT_OUTPUT_SEP;
	public static String arraysep = DEFAULT_ARRAY_SEP;

}
