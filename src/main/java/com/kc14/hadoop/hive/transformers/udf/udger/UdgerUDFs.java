package com.kc14.hadoop.hive.transformers.udf.udger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.kc14.hadoop.hive.transformers.udf.common.StaticOptionHolder;
import com.kc14.hadoop.hive.transformers.udf.common.UDFAdapter;
import com.kc14.hadoop.hive.transformers.udf.common.UDFPackageIF;
import com.kc14.java.reflect.BeanUtils;
import com.kc14.org.udger.parser.UdgerParser;
import com.kc14.org.udger.parser.UdgerUaResult;

public class UdgerUDFs extends UDFAdapter implements UDFPackageIF {
	
	private static final String PACKAGE_NAME = "udger"; 

	@Override
	public String getPackageName() {
		return PACKAGE_NAME;
	}

	private final static String OPTION_UDGER_DATABASE = "udger-database";
	private final static String OPTION_UDGER_INMEM = "udger-inmem";
	private final static String OPTION_UDGER_CACHE = "udger-cache";
	private final static String OPTION_UDGER_FIELDS = "udger-fields";
	private final static String OPTION_UDGER_LIST =   "udger-list";

	public  final static String[] ALL_FIELDS = {
			"crawlerCategory",
			"crawlerLastSeen",
			"crawlerRespectRobotstxt",
			"deviceBrand",
			"deviceBrandHomepage",
			"deviceBrandInfoUrl",
			"deviceClass",
			"deviceClassInfoUrl",
			"deviceMarketname",
			"os",
			"osFamily",
			"osFamilyVedorHomepage",
			"osFamilyVendor",
			"osHomePage",
			"osInfoUrl",
			"ua",
			"uaClass",
			"uaEngine",
			"uaFamily",
			"uaFamilyHomepage",
			"uaFamilyInfoUrl",
			"uaFamilyVendor",
			"uaFamilyVendorHomepage",
			"uaString",
			"uaUptodateCurrentVersion",
			"uaVersion",
			"uaVersionMajor"
	};

	public  final static String[] STD_FIELDS = {
			"ua",
			"uaClass",
			"uaEngine",
			"uaFamily",
			"uaVersionMajor",
			"uaVersion",
			"uaUptodateCurrentVersion",
			"deviceClass",
			"deviceBrand",
			"deviceMarketname",
			"os",
			"osFamily",
			"crawlerCategory"
	};

	@Override
	public Collection<Option> getOptions () {

		Options options = new Options();
        
        options.addOption(Option.builder()
        		.longOpt      (OPTION_UDGER_DATABASE)
        		.desc         ("udger database file")
        		.required     (true)
        		.hasArg       (true)
        		.argName      ("filename")
        		.numberOfArgs (1)
        		.type         (String.class)
        		.build());

        options.addOption(Option.builder()
        		.longOpt      (OPTION_UDGER_INMEM)
        		.desc         ("use in memory udger db")
        		.required     (false)
        		.hasArg       (false)
        		.build());

        options.addOption(Option.builder()
        		.longOpt      (OPTION_UDGER_CACHE)
        		.desc         ("cache capacity")
        		.required     (false)
        		.hasArg       (true)
        		.argName      ("capacity")
        		.numberOfArgs (1)
        		.type         (Number.class)
        		.build());

        options.addOption(Option.builder()
        		.longOpt      (OPTION_UDGER_FIELDS)
        		.desc         ("fields to return")
        		.required     (false)
        		.hasArgs      ()
        		.argName      ("field*")
        		.type         (String.class)
        		.build());
        
        options.addOption(Option.builder()
        		.longOpt      (OPTION_UDGER_LIST)
        		.desc         ("list udger fields")
        		.hasArg       (false)
        		.required     (false)
        		.build());

        return options.getOptions();
	}

	UdgerParser udgerParser = null;
	String[] fields = null;
	
	@Override
	public void initFrom(CommandLine commandLine) throws FileNotFoundException, UnsupportedEncodingException, IOException {

		// My Options
		
		// Setup reasonable defaults
		
		String   udgerDb =       null;
		boolean  inMemory =      false;
		int      cacheCapacity = 100000;
		
		this.fields = STD_FIELDS;
		
		// Set commandline options
		
		udgerDb = commandLine.getOptionValue(OPTION_UDGER_DATABASE);
		System.err.println("info: udf-transformer-groovy: UdgerUDFs.initFrom(): Using Database: " + udgerDb);
		inMemory = commandLine.hasOption(OPTION_UDGER_INMEM);
		if (commandLine.hasOption(OPTION_UDGER_CACHE)) cacheCapacity = Integer.parseInt(commandLine.getOptionValue(OPTION_UDGER_CACHE));
        if (commandLine.hasOption(OPTION_UDGER_FIELDS)) this.fields = commandLine.getOptionValues(OPTION_UDGER_FIELDS);
		
		if (commandLine.hasOption(OPTION_UDGER_LIST)) {
			System.err.println("UdgerUDFs: Known Fields: " + Arrays.toString(ALL_FIELDS));
		}
		
		// Let's rock
		this.udgerParser = new UdgerParser (udgerDb, inMemory, cacheCapacity);
	}

	public List<String> parseUa(String value) {
		UdgerUaResult udgerUaResult = null;
		try {
			// udgerUaResult = this.udgerParser.parseUa("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9");
			udgerUaResult = this.udgerParser.parseUa(value);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		Map<String, String> udgerUaResultProperties = BeanUtils.getBeanProperties(udgerUaResult);
		List<String> udgerUaResultPropertyValues = Arrays.asList(this.fields).stream().map(field -> udgerUaResultProperties.get(field)).collect(Collectors.toList());
		return udgerUaResultPropertyValues; 
	}

}
