package com.kc14.hadoop.hive.transformers.udf;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.kc14.hadoop.hive.transformers.udf.common.UDFCollection;
import com.kc14.hadoop.hive.transformers.udf.common.UDFTransformerGroovy;

public class UniversalTransformer {
	
	public static void main(String[] args) throws Exception {
		
		// UniversalTransformer Arguments & Option Parsing

		// First Argument is csv list of UDF packages to use ...
		
		if (args.length < 1) {
			System.err.println("error: udf-transformer-groovy: UniversalTransformer.main(): The first argument must be a comma separated list of UDF packages classes to load, e.g. [com.kc14.hadoop.hive.transformers.udf.common.CommonUDFs].");
			System.exit(99);
		}

		String[] udfPackagesToLoadByName = args[0].split(",");

		String[] shiftedArgs = Arrays.copyOfRange(args, 1, args.length);


		// Option Parsing
		
		Options options = new Options(); // Our own options ... currently none
		
		UDFCollection udfCollection = new UDFCollection(udfPackagesToLoadByName);
		
		Collection<Option> udfOptions = udfCollection.getOptions(); // Options from the UDF Packages

		for (Option udfOption : udfOptions) {
			options.addOption(udfOption);
		}

		// Let's rock
		
		UDFTransformerGroovy udfTransformerGroovy = new UDFTransformerGroovy(udfCollection);

		CommandLine commandLine = udfTransformerGroovy.parse(shiftedArgs, options);
		
		udfCollection.initFrom(commandLine);
		
		udfTransformerGroovy.run();

	}

}
