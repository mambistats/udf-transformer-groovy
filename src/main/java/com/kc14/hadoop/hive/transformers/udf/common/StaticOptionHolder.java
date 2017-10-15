package com.kc14.hadoop.hive.transformers.udf.common;

import java.util.Properties;

public class StaticOptionHolder {

	private static final String PROPERTY_INPUT_SEP =           "transformer.input.sep";
	private static final String PROPERTY_INPUT_ARR_ELEM_SEP =  "transformer.input.array.elem.sep";

	private static final String PROPERTY_OUTPUT_SEP =          "transformer.output.sep";
	private static final String PROPERTY_OUTPUT_ARR_ELEM_SEP = "transformer.output.array.elem.sep";
	
	private static final String PROPERTY_OUTPUT_ESC_ENABLE =   "transformer.output.esc.enable";
	private static final String PROPERTY_ARRAY_ESC_ENABLE =    "transformer.array.esc.enable";

	private static final char   DEFAULT_INPUT_SEP =           '\t';      // I.e. ASCII TAB
	private static final char   DEFAULT_INPUT_ARR_ELEM_SEP =  '\u0002';  // I.e. ASCII STX (Start of Text, x02)
	private static final char   DEFAULT_OUTPUT_SEP =          '\t';      // I.e. ASCII TAB
	private static final char   DEFAULT_OUTPUT_ARR_ELEM_SEP = '\u0002';  // I.e. ASCII STX (Start of Text, x02)
	private static final String DEFAULT_HIVE_NULL_STR =       "\\N";     // I.e. a single backslash + capital N
	
	public static final String[] STRING_ARRAY = new String[0];
	
	public static char    inputSep =         DEFAULT_INPUT_SEP;
	public static char    inputArrElemSep =  DEFAULT_INPUT_ARR_ELEM_SEP;
	public static char    outputSep =        DEFAULT_OUTPUT_SEP;
	public static char    outputArrElemSep = DEFAULT_OUTPUT_ARR_ELEM_SEP;
	public static String  hiveNull =         DEFAULT_HIVE_NULL_STR;
	
	public static String toUnicodeEscape (char c) {
		return String.format("\\u%04x", (int) c);
	}

	public static boolean isOutputEscEnabled = false;
	public static boolean isArrayEscEnabled = false;
	public static String  ucEscOutputArrElemSep = toUnicodeEscape(DEFAULT_OUTPUT_ARR_ELEM_SEP);
	public static String  ucEscOutputSep = toUnicodeEscape(DEFAULT_OUTPUT_SEP);;
	
	public static Properties properties = new Properties();

	public static void setProperties(Properties properties) {
		StaticOptionHolder.properties = properties;
		
		StaticOptionHolder.inputSep =         properties.getProperty(PROPERTY_INPUT_SEP,           Character.toString(DEFAULT_INPUT_SEP)).charAt(0);
		StaticOptionHolder.inputArrElemSep =  properties.getProperty(PROPERTY_INPUT_ARR_ELEM_SEP,  Character.toString(DEFAULT_INPUT_ARR_ELEM_SEP)).charAt(0);

		StaticOptionHolder.outputSep =        properties.getProperty(PROPERTY_OUTPUT_SEP,          Character.toString(DEFAULT_OUTPUT_SEP)).charAt(0);
		StaticOptionHolder.outputArrElemSep = properties.getProperty(PROPERTY_OUTPUT_ARR_ELEM_SEP, Character.toString(DEFAULT_OUTPUT_ARR_ELEM_SEP)).charAt(0);

		StaticOptionHolder.ucEscOutputSep = toUnicodeEscape(StaticOptionHolder.outputSep);
		StaticOptionHolder.ucEscOutputArrElemSep = toUnicodeEscape(StaticOptionHolder.outputArrElemSep);
		
		StaticOptionHolder.isOutputEscEnabled = Boolean.valueOf(properties.getProperty(PROPERTY_OUTPUT_ESC_ENABLE));
		StaticOptionHolder.isArrayEscEnabled = Boolean.valueOf(properties.getProperty(PROPERTY_ARRAY_ESC_ENABLE));
	}

}
