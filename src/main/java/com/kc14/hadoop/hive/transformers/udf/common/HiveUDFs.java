package com.kc14.hadoop.hive.transformers.udf.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import javax.crypto.CipherInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

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
	
	private static int countEscapes(String s, char toEscape) {
		int countEscapes = 0;
		for (char c : s.toCharArray()) if (c == toEscape) ++countEscapes;
		if (countEscapes > 0 && StaticOptionHolder.isOutputEscapingEnabled == false) throw new IllegalArgumentException(String.format("Found character to escape [%c] in [%s] but escaping is disabled", toEscape, s));
		return countEscapes;
	}
	
	private static String escapeChar(String s, char toEscape) {
		int countEscapes = countEscapes(s, toEscape);
		
		if (countEscapes == 0) return s;

		char[] r = new char[s.length() + countEscapes];
		int i = 0;
		for (char c : s.toCharArray()) {
			if (c == StaticOptionHolder.outputsep) r[i++] = StaticOptionHolder.outputesc;
			r[i++] = c;
		}
		return new String(r, 0, i);
	}
	
	public static String encodeHiveString(String s) {
		return escapeChar(s, StaticOptionHolder.outputsep);
	}

	public static String encodeHiveArrayElement(String s) {
		return escapeChar(s, StaticOptionHolder.outputarr);
	}

	public static int colToRowIdx(int col) {
		return col - 1;
	}
	
	public static String toHiveArray (Object[] a) {
		String[] b = new String[a.length];
		for (int i = 0; i < a.length; ++i) {
			if (a[i] == null) b[i] = StaticOptionHolder.hivenull;
			else b[i] = encodeHiveArrayElement(String.valueOf(a[i])); // Convert object to its string representation
		}
		return String.join(Character.toString(StaticOptionHolder.outputarr), b);		
	}
	
	private String[] fromHiveArray(String a) {
		return a.split(Character.toString(StaticOptionHolder.inputarr));
	}	

	// UDFs
	
	public String col(int pos) {
		return this.inputRow[colToRowIdx(pos)];
	}
	
	public String array(Object[] a) {
		return toHiveArray(a);
	}
	
	public String encodeArray(Object[] a) {
		return toHiveArray(a);
	}
	
	public String[] decodeArray(String a) {
		return fromHiveArray(a);
	}
	
	public String concat(String... args) { // Type of args is String[] (I.e. an Array of Strings)
		return String.join("", args);
	}
	
	public String iphex(String value) throws UnknownHostException {
		return Hex.encodeHex(IPv6UDFs.toIPv6AddressFromStrFurios(value).toByteArray());
	}

}
