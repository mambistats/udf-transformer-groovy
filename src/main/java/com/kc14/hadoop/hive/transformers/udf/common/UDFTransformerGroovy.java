package com.kc14.hadoop.hive.transformers.udf.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.commons.text.StringEscapeUtils;

public class UDFTransformerGroovy {

	private int          numOfBuffers;
	private String       selectExpr;
	private UDFPackageIF udfPackage;
	private ScriptEngine engine;
	private boolean      isFailEarly;

	private static final int    DEFAULT_NUM_BUFFERS = 1;

	public UDFTransformerGroovy(UDFPackageIF udfPackage) {
		this.numOfBuffers = DEFAULT_NUM_BUFFERS;
		this.isFailEarly = false;
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
		BufferedReader in =  new BufferedReader(new InputStreamReader(System.in),   this.numOfBuffers * 8192);
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out), this.numOfBuffers * 4096);

		// Declare local vars here to optimize gc
		String line = null;
		String[] inputRow = null;
		Object rawOutput = null;
		Iterable<?> iterableOutput = null;
		List<String> outputRow = null;
		int lineNum = 0;

		while ((line = in.readLine()) != null) {

			rawOutput = null;
			iterableOutput = null;
			outputRow = null;

			++lineNum;

			inputRow = line.split(Character.toString(StaticOptionHolder.inputSep));

			this.udfPackage.prepareInputRow(inputRow);
			this.engine.put(INPUT_ROW_NAME, inputRow);
			
			try {
				rawOutput = this.engine.eval(this.selectExpr);
			}
			catch (Exception e) {
				System.err.format("error: udf-transformer-groovy: UDFTransformerGroovy.run(): Exception during processing of line[%d]: [%s]\n", lineNum, StringEscapeUtils.escapeJava(line));
				if (this.isFailEarly) throw e;
				e.printStackTrace();
				continue;
			}
			
			if (rawOutput instanceof Iterable<?>) iterableOutput = ((Iterable<?>) rawOutput);
			else {
				ArrayList<Object> outputList = new ArrayList<Object>(1);
				outputList.add(rawOutput);
				iterableOutput = outputList;
			}
			
			outputRow = new ArrayList<>();
			
			try {
				for (Object o: iterableOutput) {
					if (o == null) outputRow.add(StaticOptionHolder.hiveNull); // Replace null by hive's null string representation
					else if (o.getClass().isArray()) outputRow.add(HiveUDFs.toHiveArray(Arrays.asList((Object[]) o)));
					else if (o instanceof Iterable<?>) outputRow.add(HiveUDFs.toHiveArray((Iterable<?>) o));
					else outputRow.add(HiveUDFs.toHiveString(o));
				}
			}
			catch (Exception e) {
				System.err.format("error: udf-transformer-groovy: UDFTransformerGroovy.run(): Exception during result output of line[%d]: [%s]\n", lineNum, StringEscapeUtils.escapeJava(line));
				System.err.format("error: udf-transformer-groovy: UDFTransformerGroovy.run(): Bad output row: %s\n", StringEscapeUtils.escapeJava(String.valueOf(rawOutput)));
				if (this.isFailEarly) throw e;
				e.printStackTrace();
				continue;
			}
			
			out.write(String.join(Character.toString(StaticOptionHolder.outputSep), outputRow));
			out.newLine();
		}
		
		// in.close(); // Hive should do that ...
		out.flush();
		out.close(); // ... but we are definitely done and need that to flush the data
	}
	
	private final static String LONG_OPTION_SELECT =       "select";
	private final static String LONG_OPTION_BUFFERS =      "buffers";
	private final static String LONG_OPTION_DEFINE =       "define";
	private final static String SHORT_OPTION_DEFINE =      "D";
	private final static String LONG_OPTION_FAIL_EARLY =   "fail-early";

	public CommandLine parse (String[] args, Options otherOptions) throws IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {

		// Option Parsing
		
		Options options = new Options();
		
		options.addOption(Option.builder()
				.longOpt      (LONG_OPTION_SELECT)
				.desc         ("select columns or UDF calls to output.\n")
				.required     (true)
				.hasArg       (true)
				.argName      ("<groovy expr>")
				.numberOfArgs (1)
				.build());

		options.addOption(Option.builder()
				.longOpt      (LONG_OPTION_BUFFERS)
				.desc         ("number of buffers to use for i/o, defaults to 1 (which is good)")
				.required     (false)
				.hasArg       (true)
				.argName      ("n")
				.numberOfArgs (1)
				.type         (Number.class)
				.build());

		options.addOption(Option.builder(SHORT_OPTION_DEFINE)
				.longOpt(LONG_OPTION_DEFINE)
				.desc         ("define property: name = value")
				.required     (false)
				.hasArgs      ()
				.argName      ("property=value")
				.numberOfArgs (2)
				.valueSeparator()
				.build());

		options.addOption(Option.builder()
				.longOpt      (LONG_OPTION_FAIL_EARLY)
				.desc         ("fail immediately at bad input line (default: false)")
				.required     (false)
				.hasArg       (false)
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
			if (commandLine.hasOption(LONG_OPTION_BUFFERS)) this.numOfBuffers = ((Number)commandLine.getParsedOptionValue("buffers")).intValue();
			StaticOptionHolder.setProperties(commandLine.getOptionProperties(LONG_OPTION_DEFINE));
			this.isFailEarly = commandLine.hasOption(LONG_OPTION_FAIL_EARLY);
			this.selectExpr = commandLine.getOptionValue(LONG_OPTION_SELECT);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			boolean autoUsageYes = true;
			formatter.printHelp("UDFTransformer", usageHeader, options, usageFooter, autoUsageYes);
			System.exit(1);
		}

		return commandLine;
		
	}
	
}
