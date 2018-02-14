package com.kc14.hadoop.hive.transformers.udf.common;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.kc14.algebra.DisjunctPartitioningIntervalMerger;
import com.kc14.janvanbesien.com.googlecode.ipv6.IPv6AddressRange;

import static com.kc14.hadoop.hive.transformers.udf.ipv6.IPv6UDFs.readNetworkRanges;
import static com.kc14.hadoop.hive.transformers.udf.ipv6.IPv6UDFs.fromLinesIPv6Ranges;

public class IPv6RangesMerger {
	
	private final static String OPTION_IPv6_RANGES = "ipv6-ranges";
	private final static String OPTION_OUTPUT = "output";

	public static void main(String[] args) throws Exception {
		
		Options options = new Options();

		options.addOption(Option.builder()
				.longOpt      (OPTION_IPv6_RANGES)
				.desc         ("GZIPed files with one tab-separated IPv6 or 4 network range per line")
				.required     (true)
				.hasArgs      ()
				.numberOfArgs (Option.UNLIMITED_VALUES)
				.type         (String.class)
				.build());

		options.addOption(Option.builder()
				.longOpt      (OPTION_OUTPUT)
				.desc         ("output as GZIPed file")
				.required     (false)
				.hasArg       ()
				.argName      ("filename")
				.type         (String.class)
				.build());

		// My Options

		String[] networkRangesFilenames = null;
		String outputFileName = null;
		
		// Prepare Option Parsing

		String usageHeader = "List of files with TSV IP Ranges\n"
				+ "Stdout: Merged index with corresponding merged fk(IP Ranges)\n";
		
		String usageFooter = "MaxMind Network Ranges Index";
		
		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine commandLine = null;

		try {
			commandLine = parser.parse(options, args);
			networkRangesFilenames = commandLine.getOptionValues(OPTION_IPv6_RANGES);
			if (commandLine.hasOption(OPTION_OUTPUT)) outputFileName = commandLine.getOptionValue(OPTION_OUTPUT);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			boolean autoUsageYes = true;
			formatter.printHelp("IPv6RangesMerger", usageHeader, options, usageFooter, autoUsageYes);
			System.exit(1);
		}
		
		IPv6RangesAndForeignKeysResult mergedNetworkRangesFiles = mergeNetworkRangesFiles (networkRangesFilenames);
		
		if (outputFileName != null) {
			PrintStream outputPrintStream = new PrintStream(new GZIPOutputStream(new FileOutputStream(outputFileName)));
			outputRanges(mergedNetworkRangesFiles, outputPrintStream);
			outputPrintStream.close();
		}
		else {
			outputRanges(mergedNetworkRangesFiles, System.out);
		}

	}


	private static IPv6RangesAndForeignKeysResult mergeNetworkRangesFiles(String[] networkRangesFilenames) throws FileNotFoundException, UnsupportedEncodingException, IOException {
		// LHS
		String firstNetworkRangesFileName = networkRangesFilenames[0];
		IPv6RangesAndForeignKeysResult firstRangesAndForeigneKeys = createRangesAndForeigneKeys(firstNetworkRangesFileName);
		List<IPv6AddressRange> A = firstRangesAndForeigneKeys.ipv6NetworkRanges;
		List<Collection<String>> AFk = firstRangesAndForeigneKeys.ipv6NetworkRangesForeignKeys;
		
		// Start with second filename as RHS
		for (int i = 1; i < networkRangesFilenames.length; ++i) {
			String rhsNetworkRangesFilename = networkRangesFilenames[i];
			IPv6RangesAndForeignKeysResult rhsRangesAndForeigneKeys = createRangesAndForeigneKeys(rhsNetworkRangesFilename);
			List<IPv6AddressRange> B = rhsRangesAndForeigneKeys.ipv6NetworkRanges;
			List<Collection<String>> BFk = rhsRangesAndForeigneKeys.ipv6NetworkRangesForeignKeys;
			
			// int estimatedInitialSize = 2 * (A.size() + B.size()) + 1; // Max: 2 * (m+n) + 1, is too prove
			int estimatedInitialSize = A.size() + B.size(); // Max: 2 * (m+n) + 1, is too prove
			List<IPv6AddressRange>   C = new ArrayList<>(estimatedInitialSize);
			List<Collection<String>> CFk = new ArrayList<>(estimatedInitialSize);
			ArrayList<String> prototype = new ArrayList<>(0); // Type Tagger
			
			if (false) {
				System.err.format("A: %s", Arrays.asList(A));
				System.err.println();
				System.err.format("B: %s", Arrays.asList(B));
				System.err.println();
				System.err.format("AFk: %s", Arrays.asList(AFk));
				System.err.println();
				System.err.format("BFk: %s", Arrays.asList(BFk));
				System.err.println();
			}

			DisjunctPartitioningIntervalMerger.merge(A.iterator(), B.iterator(), AFk.iterator(), BFk.iterator(), C, CFk, prototype);
			
			if (false) {
				System.err.format("C: %s", Arrays.asList(C));
				System.err.println();
				System.err.format("CFk: %s", Arrays.asList(CFk));
				System.err.println();
			}

			int j = 0;
			for (Collection<String> fk : CFk) {
				if (fk.size() != i+1) {
					System.err.format("Range[%d]: CFk[%d]: %s", i, j, fk);
					System.err.println();
					assert false : "foreign key collections grows";
				}
				++j;
			}
			
			// Accumulation Step
			A =   C;
			AFk = CFk;
			
			B = null;
			BFk = null;
		}
		
		return new IPv6RangesAndForeignKeysResult(A, AFk);
	}
	
	
	private static void outputRanges(IPv6RangesAndForeignKeysResult mergedNetworkRangesFiles, PrintStream outputPrintStream) {
		List<IPv6AddressRange> ipv6NetworkRanges = mergedNetworkRangesFiles.ipv6NetworkRanges;
		List<Collection<String>> ipv6NetworkRangesForeignKeys = mergedNetworkRangesFiles.ipv6NetworkRangesForeignKeys;
		Iterator<IPv6AddressRange> rangesIter = ipv6NetworkRanges.iterator();
		Iterator<Collection<String>> predsIter = ipv6NetworkRangesForeignKeys.iterator();
		while (rangesIter.hasNext() && predsIter.hasNext()) {
			IPv6AddressRange range = rangesIter.next();
			Collection<String> foreignKeys = predsIter.next();
			String firstAddr = range.getFirst().toShortString();
			String lastAddr =  range.getLast().toShortString();
			outputPrintStream.format("%s\t%s", firstAddr, lastAddr);
			for (String foreignKey : foreignKeys) {
				if (foreignKey == null) outputPrintStream.format("\t%s", StaticOptionHolder.hiveNull);
				else outputPrintStream.format("\t%s", foreignKey);
			}
			outputPrintStream.println();
		}
	}


	private static class IPv6RangesAndForeignKeysResult {
		public IPv6RangesAndForeignKeysResult(List<IPv6AddressRange> ipv6NetworkRanges, List<Collection<String>> ipv6NetworkRangesForeignKeys) {
			this.ipv6NetworkRanges = ipv6NetworkRanges;
			this.ipv6NetworkRangesForeignKeys = ipv6NetworkRangesForeignKeys;
		}
		List<IPv6AddressRange>   ipv6NetworkRanges;
		List<Collection<String>> ipv6NetworkRangesForeignKeys;
	}

	
	private static IPv6RangesAndForeignKeysResult createRangesAndForeigneKeys(String firstNetworkRangesFileName) throws FileNotFoundException, IOException, UnsupportedEncodingException, UnknownHostException {
		List<String> networkRangeLines = readNetworkRanges(firstNetworkRangesFileName);
		IPv6AddressRange[] ipv6NetworkRangesAsArray = fromLinesIPv6Ranges(networkRangeLines);
		List<IPv6AddressRange> ipv6NetworkRanges = Arrays.asList(ipv6NetworkRangesAsArray);
		ArrayList<Collection<String>> ipv6NetworkRangesForeignKeys = new ArrayList<>(ipv6NetworkRanges.size()); // The first address of a range is the needed foreign key
		for (IPv6AddressRange ipv6AddressRange : ipv6NetworkRanges) {
			ipv6NetworkRangesForeignKeys.add(Arrays.asList(ipv6AddressRange.getFirst().toShortString()));
		}
		return new IPv6RangesAndForeignKeysResult(ipv6NetworkRanges, ipv6NetworkRangesForeignKeys);
	}

}
