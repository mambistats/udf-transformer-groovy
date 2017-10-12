package com.kc14.hadoop.hive.transformers.udf.ipv6;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.kc14.hadoop.codec.binary.Hex;
import com.kc14.hadoop.hive.transformers.udf.common.UDFAdapter;
import com.kc14.hadoop.hive.transformers.udf.common.UDFPackageIF;
import com.kc14.janvanbesien.com.googlecode.ipv6.IPv6Address;
import com.kc14.janvanbesien.com.googlecode.ipv6.IPv6AddressRange;

public class IPv6UDFs extends UDFAdapter implements UDFPackageIF {
	
	private static final String PACKAGE_NAME = "ipv6"; 
	
	@Override
	public String getPackageName() {
		return PACKAGE_NAME;
	}

	private final static String OPTION_IPv6_RANGES = "ipv6-ranges";
	private final static String OPTION_IPv6_LIST =   "ipv6-list";
	
	private final static String KNOWN_UDFS = "IPv6 Transformation UDFs:\n"
			+ "   ip\n"
			+ "   start\n"
			+ "   last\n"
			+ "   next\n"
			+ "   iphex\n"
			+ "   starthex\n"
			+ "   lasthex\n"
			+ "   nexthex\n";

	@Override
	public Collection<Option> getOptions () {

		Options options = new Options();

		options.addOption(Option.builder()
				.longOpt      (OPTION_IPv6_RANGES)
				.desc         ("GZIPed file with one tab-separated IPv6 or 4 network range per line")
				.required     (true)
				.hasArg       (true)
				.argName      ("filename")
				.numberOfArgs (1)
				.type         (String.class)
				.build());

		options.addOption(Option.builder()
				.longOpt      (OPTION_IPv6_LIST)
				.desc         ("list UDFs provided by this package")
				.hasArg       (false)
				.required     (false)
				.build());

		return options.getOptions();

	}

	IPv6AddressRange[] ipv6NetworkRanges = null;

	@Override
	public void initFrom(CommandLine commandLine) throws FileNotFoundException, UnsupportedEncodingException, IOException {

		// My Options
		String networkRangesFilename = null;
		networkRangesFilename = commandLine.getOptionValue(OPTION_IPv6_RANGES);
		
		if (commandLine.hasOption(OPTION_IPv6_LIST)) {
			System.err.println(KNOWN_UDFS);
		}
		
		// Let's rock
		ArrayList<String> networkRangeLines = readNetworkRanges(networkRangesFilename);
		IPv6AddressRange[] ipv6NetworkRanges = fromLinesIPv6Ranges(networkRangeLines);
		// UDFIF ipv6UDFs = new IPv6UDFsWithCache(ipv6NetworkRanges);
		this.ipv6NetworkRanges = ipv6NetworkRanges;
	}

	private static ArrayList<String> readNetworkRanges(String networkRangesFilename) throws FileNotFoundException, IOException, UnsupportedEncodingException {
		System.err.println("info: udf-transformer-groovy: IPv6UDFs.readNetworkRanges(): Reading file [" + networkRangesFilename + "] ...");
		FileInputStream is = new FileInputStream(networkRangesFilename);
		GZIPInputStream gzis = new GZIPInputStream(is, 16 * 4096); // This is a buffered input stream
		InputStreamReader reader = new InputStreamReader(gzis, "UTF-8");
		BufferedReader in = new BufferedReader(reader, 2 * 8192);
		String line = null;
		ArrayList<String> networkRangeLines = new ArrayList<>(16777216); // 2**24 = 16777216
		while ((line = in.readLine()) != null) {
			networkRangeLines.add(line);
		}
		in.close();
		return networkRangeLines;
	}

	// Inet4Address to long

	static final byte[] HIGHBYTES = Hex.decodeHex("00000000");
	
	public static long inet4AddressToLong (Inet4Address inet4Address) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE).order(ByteOrder.BIG_ENDIAN);
		buffer.put(HIGHBYTES);
		buffer.put(inet4Address.getAddress());
		buffer.position(0);
		return buffer.getLong();
	}
	
	// Failsafe IPv4 Dotted Notation Converter

	private final static long[] pow256 = {
			0,               // 256^0
			256,             // 256^1
			256 * 256,       // 256^2
			256 * 256 * 256  // 256^3
	};
	
	public static long toLongDottedIPv4Address (String ipAddr) throws NumberFormatException {
		String[] ipAddrSegs = ipAddr.split("\\.");
		long result = 0;
		for (int i = 0; i < ipAddrSegs.length; i++) {
			int power = 3 - i;
			int seg = Integer.parseInt(ipAddrSegs[i]);
			result += seg * pow256[power];
		}
		return result;
	}

	// Inet4Address to IPV6Address

	static final byte[] IPv6_BITS_63_TO_32_FOR_MAPPED_IPv4 = Hex.decodeHex("0000FFFF");
	
	public static IPv6Address inet4AddressToIPv6Address (Inet4Address inet4Address) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE).order(ByteOrder.BIG_ENDIAN);
		buffer.put(IPv6_BITS_63_TO_32_FOR_MAPPED_IPv4);
		buffer.put(inet4Address.getAddress());
		buffer.position(0);
		long lowBits = buffer.getLong();
		long highBits = 0;
		return IPv6Address.fromLongs(highBits, lowBits);
	}
	
	// Numerical IP address to IPv6Address Converter

	public static IPv6Address toIPv6AddressFromStrFurios(String ipaddr) throws UnknownHostException {
		InetAddress inetAddr = InetAddress.getByName(ipaddr);
		if (inetAddr instanceof Inet4Address) return inet4AddressToIPv6Address((Inet4Address)inetAddr); // Use mapped IPv4
		return IPv6Address.fromInetAddress(inetAddr);
	}
	
	public static IPv6Address toIPv6AddressFromStrFaster(String ipAddr) throws UnknownHostException {
		long ipv4;
		try {
			ipv4 = toLongDottedIPv4Address(ipAddr);
		}
		catch (NumberFormatException e) {
			ipv4 = -1; // Bad dot notation
		}
		if (ipv4 >= 0) return IPv6Address.fromString("::FFFF:" + ipAddr); // This Ctor is soooo slow ...
		return IPv6Address.fromString(ipAddr);
	}

	// hex to IPv6Address converter

	private static IPv6Address toIPv6AddressFromHexStr(String hexAsStr) throws UnknownHostException {
		byte[] ipv6hex = Hex.decodeHex(hexAsStr);
		return IPv6Address.fromByteArray(ipv6hex);
	}

	private static IPv6Address toIPv6AddressFromHexStrSlow(String hexAsStr) throws UnknownHostException {
		byte[] ipv6hex = DatatypeConverter.parseHexBinary(hexAsStr);
		return IPv6Address.fromByteArray(ipv6hex);
	}

	private static IPv6AddressRange[] fromLinesIPv6Ranges(List<String> networkRangeLines) throws UnknownHostException {
		int numLines = networkRangeLines.size();
		
		System.err.format("info: udf-transformer-groovy: IPv6UDFs.fromLinesIPv6Ranges(): Converting network ranges to IPv6Ranges[%,d] for binary search ...\n", numLines);
		
		IPv6AddressRange[] ipv6NetworkRanges = new IPv6AddressRange[numLines];
		int i = -1;
		for (String networkRangeLine: networkRangeLines) {
			++i;
			String[] splits = networkRangeLine.trim().split("\t");
			IPv6Address networkStart = null;
			IPv6Address networkLast = null;
			try {
				networkStart = toIPv6AddressFromStrFurios(splits[0].trim());
				networkLast = toIPv6AddressFromStrFurios(splits[1].trim());
				ipv6NetworkRanges[i] = IPv6AddressRange.fromFirstAndLast(networkStart, networkLast);
			}
			catch (ArrayIndexOutOfBoundsException e) {
				System.err.format("fatal: udf-transformer-groovy: IPv6UDFs.fromLinesIPv6Ranges(): fatal error[exit(1)]: row[%,d]: [%s]: bad IPv6AddressRange\n", i+1, networkRangeLine);
				System.exit(1);
			}
			catch (UnknownHostException e) {
				System.err.format("fatal: udf-transformer-groovy: IPv6UDFs.fromLinesIPv6Ranges(): fatal error[exit(2)]: row[%,d]: [%s]: bad IPv6Addresses\n", i+1, networkRangeLine);
				System.exit(2);
			}
		}
		
		System.err.format("info: udf-transformer-groovy: IPv6UDFs.fromLinesIPv6Ranges(): Sorting IPv6Ranges[%,d] for binary search ...\n", numLines);
		
		Arrays.sort(ipv6NetworkRanges, new Comparator<IPv6AddressRange>() {
			
			@Override
			public int compare(IPv6AddressRange r1, IPv6AddressRange r2) {
				return r1.getFirst().compareTo(r2.getFirst());
			}

		});
		
		System.err.println("info: udf-transformer-groovy: IPv6UDFs.fromLinesIPv6Ranges(): Ready for lookup and transform.");

		return ipv6NetworkRanges;
	}
	
	// Business Logic

	public IPv6AddressRange[] getIPv6NetworkRanges() {
		return this.ipv6NetworkRanges;
	}
	
	// Binary search in array

	/**
	 * Find the maximal element lesser than or equal to the element in question
	 *
	 * @param a   An array of long values in <em>ascending</em> order
	 * @param n   The number for which to find the maximal element
	 * @return    The index of the maximal element or -1 if there is no such element
	 */
	public static int findIdxOfGreatestValueLesserThanOrEqualToN(long[] a, long n) {
		int lo = 0;
		int hi = a.length - 1;
		// int loops = 0;
		while (lo <= hi) {
			// ++loops;
			// Key is in a[lo..hi] or not present.
			int mid = lo + (hi - lo) / 2;
			if      (n < a[mid]) hi = mid - 1;
			else if (n > a[mid]) lo = mid + 1;
			else return mid; // Equality Match!
		}
		// System.err.println("k: " + n + ", loops: " + loops);
		if (hi < 0) return -1; // Less than lowest
		else if (lo >= a.length) return a.length - 1; // Greater than highest
		else return hi; // Greatest lower element
	}

	/**
	 * Find the maximal element lesser than or equal to the element in question
	 *
	 * @param ipv6AddressRanges   An array of long values in <em>ascending</em> order
	 * @param n   The number for which to find the maximal element
	 * @return    The index of the maximal element or -1 if there is no such element
	 */
	public static int findIdxOfGreatest_IPv6AddressRangeFirst_LesserThanOrEqualTo(IPv6AddressRange[] ipv6AddressRanges, IPv6Address ipv6Address) {
		int lo = 0;
		int hi = ipv6AddressRanges.length - 1;
		// int loops = 0;
		while (lo <= hi) {
			// ++loops;
			// Key is in a[lo..hi] or not present. Remember r=compareTo(a,b) => (r<0) means a<b, (r==0) means a==b, (r>0) means a>b
			int mid = lo + (hi - lo) / 2;
			if      (ipv6Address.compareTo(ipv6AddressRanges[mid].getFirst()) < 0) hi = mid - 1;
			else if (ipv6Address.compareTo(ipv6AddressRanges[mid].getFirst()) > 0) lo = mid + 1;
			else return mid; // Equality Match!
		}
		// System.err.println("k: " + n + ", loops: " + loops);
		if (hi < 0) return -1; // Less than lowest
		else if (lo >= ipv6AddressRanges.length) return ipv6AddressRanges.length - 1; // Greater than highest
		else return hi; // Greatest lower element
	}
	

	// IPv6 Address Converters

	static final IPv6Address ipv6null = IPv6Address.fromLongs(0, 0);

	private static IPv6Address toIPv6AddressFromStr(String s) throws UnknownHostException {
		InetAddress inetAddr = InetAddress.getByName(s);
		if (inetAddr instanceof Inet4Address) return IPv6Address.fromString("::FFFF:" + ((Inet4Address) inetAddr).getHostAddress()); // Use mapped IPv4
		return IPv6Address.fromInetAddress(inetAddr);
	}

	// UDF Helpers

	// Prepare row for processing

	// @Override
	// public void setInputRow(String[] inputRow) {
	// 	super.setInputRow(inputRow);
	// }
	
	// Implementations

	public IPv6Address getIp(String value) throws UnknownHostException {
		return toIPv6AddressFromStrFurios(value);
	}

	public IPv6Address getStart(String value) throws UnknownHostException {
		IPv6Address ip = toIPv6AddressFromStr(value);
		int n = findIdxOfGreatest_IPv6AddressRangeFirst_LesserThanOrEqualTo (this.ipv6NetworkRanges, ip);
		if (n < 0) { // Lower than first range
			return null;
		}
		else { // Range hit
			IPv6AddressRange range = this.ipv6NetworkRanges[n]; // Remember: Maximum start but range MAY NOT contain ip!
			return range.contains(ip) ? range.getFirst() : null;
		}
	}

	public IPv6Address getLast(String value) throws UnknownHostException {
		IPv6Address ip = toIPv6AddressFromStr(value);
		int n = findIdxOfGreatest_IPv6AddressRangeFirst_LesserThanOrEqualTo (this.ipv6NetworkRanges, ip);
		if (n < 0) { // Lower than first range
			return null;
		}
		else { // Range hit
			return this.ipv6NetworkRanges[n].getLast(); // Remember: Maximum start but range MAY NOT contain ip!
		}
	}

	public IPv6Address getNext(String value) throws UnknownHostException {
		IPv6Address ip = toIPv6AddressFromStr(value);
		int n = findIdxOfGreatest_IPv6AddressRangeFirst_LesserThanOrEqualTo (this.ipv6NetworkRanges, ip);
		if (n < 0) { // Lower than first range
			return this.ipv6NetworkRanges.length > 0 ? this.ipv6NetworkRanges[0].getFirst() : null;
		}
		else { // Range hit
			return n+1 < this.ipv6NetworkRanges.length ? this.ipv6NetworkRanges[n+1].getFirst() : null; // Next range start or null if no next range
		}
	}
	
	// The dyn UDFs
	
	public String ip(String value) throws UnknownHostException {
		return this.getIp(value).toShortString();
	}

	public String start(String value) throws UnknownHostException {
		return this.getStart(value).toShortString();
	}

	public String last(String value) throws UnknownHostException {
		return this.getLast(value).toShortString();
	}

	public String next(String value) throws UnknownHostException {
		return this.getNext(value).toShortString();
	}
	
	public String iphex(String value) throws UnknownHostException {
		return Hex.encodeHex(this.getIp(value).toByteArray());
	}

	public String starthex(String value) throws UnknownHostException {
		return Hex.encodeHex(this.getStart(value).toByteArray());
	}

	public String lasthex(String value) throws UnknownHostException {
		return Hex.encodeHex(this.getLast(value).toByteArray());
	}

	public String nexthex(String value) throws UnknownHostException {
		return Hex.encodeHex(this.getNext(value).toByteArray());
	}
	
}
