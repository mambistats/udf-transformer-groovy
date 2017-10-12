package com.kc14.hadoop.hive.transformers.udf.common;

import java.lang.Exception;
import java.util.Collection;

import javax.script.ScriptEngine;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public interface UDFPackageIF {

	String getPackageName();

	Collection<Option> getOptions();

	void initFrom(CommandLine commandLine) throws Exception;
	
	void putInto(ScriptEngine engine);
	
	void prepareInputRow (String[] inputRow);
	
}
