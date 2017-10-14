package com.kc14.hadoop.hive.transformers.udf.common;

public class StaticOptionHolder {
	
	private static final char   DEFAULT_INPUT_SEP =     '\t';      // I.e. ASCII TAB
	private static final char   DEFAULT_INPUT_ESC =     '\\';      // I.e. a single backslash
	private static final char   DEFAULT_INPUT_ARR =     '\u0002';  // I.e. ASCII STX (Start of Text, x02)
	private static final char   DEFAULT_OUTPUT_SEP =    '\t';      // I.e. ASCII TAB
	private static final char   DEFAULT_OUTPUT_ESC =    '\\';      // Escaping is disabled by default
	private static final char   DEFAULT_OUTPUT_ARR =    '\u0002';  // I.e. ASCII STX (Start of Text, x02)
	private static final String DEFAULT_HIVE_NULL_STR = "\\N";     // I.e. a single backslash + capital N
	
	public static final String[] STRING_ARRAY = new String[0];
	
	public static char    inputsep =  DEFAULT_INPUT_SEP;
	public static char    inputesc =  DEFAULT_INPUT_ESC;
	public static char    inputarr =  DEFAULT_INPUT_ARR;
	public static char    outputsep = DEFAULT_OUTPUT_SEP;
	public static char    outputesc = DEFAULT_OUTPUT_ESC;
	public static char    outputarr = DEFAULT_OUTPUT_ARR;
	public static String  hivenull =  DEFAULT_HIVE_NULL_STR;

	public static boolean isIntputEscapingEnabled = false;
	public static boolean isOutputEscapingEnabled = false;

}
