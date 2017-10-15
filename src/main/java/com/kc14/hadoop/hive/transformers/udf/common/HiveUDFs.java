package com.kc14.hadoop.hive.transformers.udf.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.text.StringEscapeUtils;

import com.kc14.hadoop.codec.binary.Hex;
import com.kc14.hadoop.hive.transformers.udf.ipv6.IPv6UDFs;

public class HiveUDFs extends UDFAdapter implements UDFPackageIF {

	private static final String PACKAGE_NAME = "hive";
	@Override
	public String getPackageName() {
		return PACKAGE_NAME;
	}

	@Override
	public Collection<Option> getOptions() {
		return Collections.emptyList();
	}
	
	@Override
	public void initFrom(CommandLine commandLine) throws FileNotFoundException, UnsupportedEncodingException, IOException {
	}
	
	private String[] inputRow; 

	@Override
	public void prepareInputRow(String[] inputRow) {
		this.inputRow = inputRow;
	}
	
	// Utilities - can also be used by other UDF packages
	
	public static String toHiveString(Object o) {
		String s = String.valueOf(o);
		if (StaticOptionHolder.isOutputEscEnabled) return StringEscapeUtils.escapeJava(s);			
		return s;
	}

	private static String escapeArrayElement(Object o) {
		String s = StringEscapeUtils.escapeJava(String.valueOf(String.valueOf(o))); // Escape tab newline etc
		String elemSepReplaced = s.replace(Character.toString(StaticOptionHolder.outputArrElemSep), StaticOptionHolder.ucEscOutputArrElemSep); // Escape array element separator
		String outputSepReplaced = elemSepReplaced.replace(Character.toString(StaticOptionHolder.outputSep), StaticOptionHolder.ucEscOutputSep); // Escape array element separator
		return outputSepReplaced;
	}
	
	private static String rawArrayElement(Object o) {
		String s = String.valueOf(o);
		if (s.indexOf(StaticOptionHolder.outputArrElemSep) >= 0) throw new IllegalArgumentException(String.format("Array element [%s] contains array element separator [%c,\\x04x], but escaping is disabled", StringEscapeUtils.escapeJava(s), StaticOptionHolder.outputArrElemSep, (int) StaticOptionHolder.outputArrElemSep));
		if (s.indexOf(StaticOptionHolder.outputSep) >= 0) throw new IllegalArgumentException(String.format("Array element [%s] contains output separator [%c,\\x04x], but escaping is disabled", StringEscapeUtils.escapeJava(s), StaticOptionHolder.outputSep, (int) StaticOptionHolder.outputSep));
		return s;
	}

	public static String toHiveArray (Iterable<?> iterableToArray) {
		ArrayList<String> r = new ArrayList<>();
		for (Object o: iterableToArray) {
			if (o == null) r.add(StaticOptionHolder.hiveNull);
			else if (StaticOptionHolder.isArrayEscEnabled) r.add(escapeArrayElement(o));
			else r.add(rawArrayElement(o));
		}
		return String.join(Character.toString(StaticOptionHolder.outputArrElemSep), r);		
	}

	private String[] fromHiveArray(String a) {
		return a.split(Character.toString(StaticOptionHolder.inputArrElemSep));
	}	

	public static int colToRowIdx(int col) {
		return col - 1;
	}
	
	// UDFs
	
	public String col(int pos) {
		return this.inputRow[colToRowIdx(pos)];
	}
	
	public String unesc(String s) {
		return StringEscapeUtils.unescapeJava(s);
	}
	
	public String array(Iterable<Object> a) {
		return toHiveArray(a);
	}
	
	public String encodeArray(Iterable<Object> a) {
		return toHiveArray(a);
	}
	
	public String[] decodeArray(String s) {
		return fromHiveArray(s);
	}
	
	public String concat(String... args) { // Type of args is String[] (I.e. an Array of Strings)
		return String.join("", args);
	}
	
	public String iphex(String value) throws UnknownHostException {
		return Hex.encodeHex(IPv6UDFs.toIPv6AddressFromStrFurios(value).toByteArray());
	}

}
