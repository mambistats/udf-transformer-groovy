package com.kc14.hadoop.hive.transformers.udf.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class UDFTransformerGroovy {

	private int          numOfBuffers;
	private String       selectExpr;
	private UDFPackageIF udfPackage;
	private ScriptEngine engine;

	private static final int    DEFAULT_NUM_BUFFERS = 1;

	public UDFTransformerGroovy(UDFPackageIF udfPackage) {
		this.numOfBuffers = DEFAULT_NUM_BUFFERS;
		this.engine = createGroovyEngine();
		this.udfPackage = udfPackage;
		this.putIntoEngine(udfPackage);
	}

	public static ScriptEngine createGroovyEngine () {
		ScriptEngineManager engineFactory = new ScriptEngineManager();
		ScriptEngine groovyEngine = engineFactory.getEngineByName("groovy");
		return groovyEngine;
	}

	private void putIntoEngine(UDFPackageIF udfPackage) {
		udfPackage.putInto(this.engine);
	}

	// Hive transformer loop
	
	private final static String INPUT_ROW_NAME = "c";

	public void run() throws ScriptException, IOException {
		// Read ipaddr from stdin, get ip network start
		// Output: ip, family, normalized_ip, ipaddrAsLongStr, networt_start
		BufferedReader in =  new BufferedReader(new InputStreamReader(System.in),   this.numOfBuffers * 8192);
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), this.numOfBuffers * 4096);

		// Declare local vars here to optimize gc
		String line = null;
		String[] inputRow = null;
		List<String> outputRow = null;

		while ((line = in.readLine()) != null) {
			inputRow = line.split(StaticOptionHolder.inputsep);
			this.udfPackage.prepareInputRow(inputRow);
			this.engine.put(INPUT_ROW_NAME, inputRow);

			outputRow = (List<String>) this.engine.eval(this.selectExpr);

			out.write(String.join(StaticOptionHolder.outputsep, outputRow));
			out.newLine();
		}
		// in.close(); // Hive should do that ...
		out.flush();
		out.close(); // ... but we are definitely done and need that to flush the data
	}
	
	private final static String OPTION_SELECT =       "select";
	private final static String OPTION_BUFFERS =      "buffers";
	private final static String OPTION_INPUT_SEP =    "input-sep";
	private final static String OPTION_OUTPUT_SEP =   "output-sep";
	private final static String OPTION_ARRAY_SEP =    "array-sep";

	public CommandLine parse (String[] args, Options otherOptions) throws IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {

		// Option Parsing
		
		Options options = new Options();
		
		options.addOption(Option.builder()
				.longOpt      (OPTION_SELECT)
				.desc         ("select columns or UDF calls to output.\n")
				.required     (true)
				.hasArg       (true)
				.argName      ("<groovy expr>")
				.numberOfArgs (1)
				.build());

		options.addOption(Option.builder()
				.longOpt      (OPTION_BUFFERS)
				.desc         ("number of buffers to use for i/o, defaults to 1 (which is good)")
				.required     (false)
				.hasArg       (true)
				.argName      ("n")
				.numberOfArgs (1)
				.type         (Number.class)
				.build());

		options.addOption(Option.builder()
				.longOpt      (OPTION_INPUT_SEP)
				.desc         ("input separator (default [\\t]")
				.required     (false)
				.hasArg       (true)
				.argName      ("separator")
				.numberOfArgs (1)
				.build());

		options.addOption(Option.builder()
				.longOpt      (OPTION_OUTPUT_SEP)
				.desc         ("output separator (default [\\t]")
				.required     (false)
				.hasArg       (true)
				.argName      ("separator")
				.numberOfArgs (1)
				.build());

		options.addOption(Option.builder()
				.longOpt      (OPTION_ARRAY_SEP)
				.desc         ("array element separator (default: [<STX>], i.e. ASCII Control Character <Start of Text> = 0x02)")
				.required     (false)
				.hasArg       (true)
				.argName      ("separator")
				.numberOfArgs (1)
				.build());

		String usageHeader = "Stdin: Line with Hive TSV\n"
				+ "Stdout: Hive TSV as defined by --select\n";
		
		String usageFooter = "See hive transform for more info.";
		
		// Add options from dervied transformer ...

		for (Option otherOption : otherOptions.getOptions()) {
			options.addOption(otherOption);
		}

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine commandLine = null;

		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption(OPTION_INPUT_SEP)) StaticOptionHolder.inputsep = commandLine.getOptionValue(OPTION_INPUT_SEP);
			if (commandLine.hasOption(OPTION_OUTPUT_SEP)) StaticOptionHolder.outputsep = commandLine.getOptionValue(OPTION_OUTPUT_SEP);
			if (commandLine.hasOption(OPTION_ARRAY_SEP)) StaticOptionHolder.arraysep = commandLine.getOptionValue(OPTION_ARRAY_SEP);
			if (commandLine.hasOption(OPTION_BUFFERS)) this.numOfBuffers = ((Number)commandLine.getParsedOptionValue("buffers")).intValue();
			this.selectExpr = commandLine.getOptionValue(OPTION_SELECT);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			boolean autoUsageYes = true;
			formatter.printHelp("UDFTransformer", usageHeader, options, usageFooter, autoUsageYes);
			System.exit(1);
		}

		return commandLine;
		
	}
	
}
