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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class IPv6UDFsWithCache extends UDFAdapter implements UDFPackageIF {

	private static final String PACKAGE_NAME = "ipv6c"; 

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

	IPv6AddressRange[] ipv6NetworkRanges;
	Map<String, IPv6Info> valueToIPv6InfoCache = null; // Little cache just for one row ...

	// Ctor

	public IPv6UDFsWithCache(IPv6AddressRange[] ipv6NetworkRanges) {
		super();
		this.ipv6NetworkRanges = ipv6NetworkRanges;
		this.valueToIPv6InfoCache = null;
	}

	@Override
	public Collection<Option> getOptions () {

		Options options = new Options();

		options.addOption(Option.builder()
				.longOpt      (OPTION_IPv6_RANGES)
				.desc         ("file with network ranges")
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
		this.valueToIPv6InfoCache = null;
	}

	private static ArrayList<String> readNetworkRanges(String networkRangesFilename) throws FileNotFoundException, IOException, UnsupportedEncodingException {
		System.err.println("info: udf-transformer-groovy: IPv6UDFsWithCache.readNetworkRanges(): Reading file [" + networkRangesFilename + "] ...");
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

	private static IPv6AddressRange[] fromLinesIPv6Ranges(List<String> networkRangeLines) throws UnknownHostException {
		int numLines = networkRangeLines.size();
		
		System.err.format("info: udf-transformer-groovy: IPv6UDFsWithCache.fromLinesIPv6Ranges(): Converting network ranges to IPv6Ranges[%,d] for binary search ...\n", numLines);
		
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
				System.err.format("fatal: udf-transformer-groovy: IPv6UDFsWithCache.fromLinesIPv6Ranges(): fatal error[exit(1)]: row[%,d]: [%s]: bad IPv6AddressRange\n", i+1, networkRangeLine);
				System.exit(1);
			}
			catch (UnknownHostException e) {
				System.err.format("fatal: udf-transformer-groovy: IPv6UDFsWithCache.fromLinesIPv6Ranges(): fatal error[exit(2)]: row[%,d]: [%s]: bad IPv6Addresses\n", i+1, networkRangeLine);
				System.exit(2);
			}
		}
		
		System.err.format("info: udf-transformer-groovy: IPv6UDFsWithCache.fromLinesIPv6Ranges(): Sorting IPv6Ranges[%,d] for binary search ...\n", numLines);
		
		Arrays.sort(ipv6NetworkRanges, new Comparator<IPv6AddressRange>() {
			
			@Override
			public int compare(IPv6AddressRange r1, IPv6AddressRange r2) {
				return r1.getFirst().compareTo(r2.getFirst());
			}

		});
		
		System.err.println("info: udf-transformer-groovy: IPv6UDFsWithCache.fromLinesIPv6Ranges(): Ready for lookup and transform.");

		return ipv6NetworkRanges;
	}
	
	
	// Mini Cache for col
	public class IPv6Info {
		public IPv6Info(IPv6Address ip, IPv6Address start, IPv6Address last, IPv6Address next) {
			super();
			this.ip = ip;
			this.start = start;
			this.last = last;
			this.next = next;
		}
		// If we lookup an address, we keep the short form, the range, and the following range
		IPv6Address      ip;
		IPv6Address      start;
		IPv6Address      last;
		IPv6Address      next;
	}

	// Business Logic

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
	
	public static IPv6Address toIPv6AddressFromStrFurios(String ipaddr) throws UnknownHostException {
		InetAddress inetAddr = InetAddress.getByName(ipaddr);
		if (inetAddr instanceof Inet4Address) return inet4AddressToIPv6Address((Inet4Address)inetAddr); // Use mapped IPv4
		return IPv6Address.fromInetAddress(inetAddr);
	}



	static final IPv6Address ipv6null = IPv6Address.fromLongs(0, 0);

	private static IPv6Address toIPv6AddressFromStr(String s) throws UnknownHostException {
		InetAddress inetAddr = InetAddress.getByName(s);
		if (inetAddr instanceof Inet4Address) return IPv6Address.fromString("::FFFF:" + ((Inet4Address) inetAddr).getHostAddress()); // Use mapped IPv4
		return IPv6Address.fromInetAddress(inetAddr);
	}

	private static IPv6Address toIPv6AddressFromHexStr(String hexAsStr) throws UnknownHostException {
		byte[] ipv6hex = DatatypeConverter.parseHexBinary(hexAsStr);
		return IPv6Address.fromByteArray(ipv6hex);
	}


	// UDF Helpers

	static final String HIVE_NULL_STR = "\\N";

	IPv6Info getIPv6InfoForValue(String value) {
		IPv6Address ip = null;
		IPv6Info ipv6Info = this.valueToIPv6InfoCache.get(value);
		if (ipv6Info == null) {
			try {
				ip = toIPv6AddressFromStr(value);
				int n = findIdxOfGreatest_IPv6AddressRangeFirst_LesserThanOrEqualTo (this.ipv6NetworkRanges, ip);
				if (n < 0) { // Lower than first range
					IPv6Address start = null;
					IPv6Address last = null;
					IPv6Address next = this.ipv6NetworkRanges.length > 0 ? this.ipv6NetworkRanges[0].getFirst() : null;
					ipv6Info = new IPv6Info(ip, start, last, next);
				}
				else { // Range hit
					IPv6AddressRange range = this.ipv6NetworkRanges[n]; // Remember: Maximum start but range MAY NOT contain ip!
					IPv6Address start = range.contains(ip) ? range.getFirst() : null;
					IPv6Address last = range.getLast();
					IPv6Address next = n+1 < this.ipv6NetworkRanges.length ? this.ipv6NetworkRanges[n+1].getFirst() : null; // Next range start or null if no next range
					ipv6Info = new IPv6Info(ip, start, last, next);
				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
				ipv6Info = new IPv6Info(ipv6null, null, null, null);
			}
			this.valueToIPv6InfoCache.put(value, ipv6Info);
		}
		return ipv6Info;
	}
	
	// Prepare row for processing

	@Override
	public void prepareInputRow(String[] inputRow) {
		this.valueToIPv6InfoCache = new HashMap<String, IPv6Info>(inputRow.length);
	}

	// The dyn UDFs

	public String ip(String value) {
		return this.getIPv6InfoForValue(value).ip.toShortString();
	}

	public String start(String value) {
		IPv6Info ipv6Info = this.getIPv6InfoForValue(value);
		return ipv6Info.start != null ? ipv6Info.start.toShortString() : HIVE_NULL_STR;
	}

	public String last(String value) {
		IPv6Info ipv6Info = this.getIPv6InfoForValue(value);
		return ipv6Info.last != null ? ipv6Info.last.toShortString() : HIVE_NULL_STR;
	}

	public String next(String value) {
		IPv6Info ipv6Info = this.getIPv6InfoForValue(value);
		return ipv6Info.next != null ? ipv6Info.next.toShortString() : HIVE_NULL_STR;
	}
	
	public String iphex(String value) {
		return Hex.encodeHex(this.getIPv6InfoForValue(value).ip.toByteArray());
	}

	public String starthex(String value) {
		IPv6Info ipv6Info = this.getIPv6InfoForValue(value);
		return ipv6Info.start != null ? Hex.encodeHex(ipv6Info.start.toByteArray()) : HIVE_NULL_STR;
	}

	public String lasthex(String value) {
		IPv6Info ipv6Info = this.getIPv6InfoForValue(value);
		return ipv6Info.last != null ? Hex.encodeHex(ipv6Info.last.toByteArray()) : HIVE_NULL_STR;
	}

	public String nexthex(String value) {
		IPv6Info ipv6Info = this.getIPv6InfoForValue(value);
		return ipv6Info.next != null ? Hex.encodeHex(ipv6Info.next.toByteArray()) : HIVE_NULL_STR;
	}
	
}
