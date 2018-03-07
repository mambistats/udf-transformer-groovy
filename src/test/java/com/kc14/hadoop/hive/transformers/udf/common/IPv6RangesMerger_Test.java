package com.kc14.hadoop.hive.transformers.udf.common;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class IPv6RangesMerger_Test {

	@Test
	public void a_b_MergeShouldReturn_expectedResultfile() throws Exception {
		String a_ranges = new File(this.getClass().getClassLoader().getResource("a.ranges.gz").getFile()).getAbsolutePath();
		String b_ranges = new File(this.getClass().getClassLoader().getResource("b.ranges.gz").getFile()).getAbsolutePath();
		String[] args = new String[] {
			"--ipv6-ranges",
			a_ranges,
			b_ranges,
			"--output",
			"target/actual-a+b-merged-ipv6-ranges.out.gz"
		};
		
		IPv6RangesMerger.main(args);

		File expectedFile = new File("src/test/resources/expected-a+b-merged-ipv6-ranges.out.gz");
		File actualFile = new File("target/actual-a+b-merged-ipv6-ranges.out.gz");
		assertTrue("Result content equals expected content!", FileUtils.contentEquals(expectedFile, actualFile));
	}

	@Test
	public void c_d_MergeShouldReturn_expectedResultfile() throws Exception {
		String c_ranges = new File(this.getClass().getClassLoader().getResource("c.ipv6_ranges.gz").getFile()).getAbsolutePath();
		String d_ranges = new File(this.getClass().getClassLoader().getResource("d.ipv6_ranges.gz").getFile()).getAbsolutePath();
		String[] args = new String[] {
			"--ipv6-ranges",
			c_ranges,
			d_ranges,
			"--output",
			"target/actual-c+d-merged-ipv6-ranges.out.gz"
		};
		
		IPv6RangesMerger.main(args);
		
		File expectedFile = new File("src/test/resources/expected-city+isp-merged-ipv6-ranges.out.gz");
		File actualFile = new File("target/actual-c+d-merged-ipv6-ranges.out.gz");
		assertTrue("Result content equals expected content!", FileUtils.contentEquals(expectedFile, actualFile));
	}

	@Test
	public void u_v_w_x_MergeShouldReturn_expectedResultfile() throws Exception {
		String u_ranges = new File(this.getClass().getClassLoader().getResource("bigdata/u.ipv6_ranges.gz").getFile()).getAbsolutePath();
		String v_ranges = new File(this.getClass().getClassLoader().getResource("bigdata/v.ipv6_ranges.gz").getFile()).getAbsolutePath();
		String w_ranges = new File(this.getClass().getClassLoader().getResource("bigdata/w.ipv6_ranges.gz").getFile()).getAbsolutePath();
		String x_ranges = new File(this.getClass().getClassLoader().getResource("bigdata/x.ipv6_ranges.gz").getFile()).getAbsolutePath();
		String[] args = new String[] {
			"--ipv6-ranges",
			u_ranges,
			v_ranges,
			w_ranges,
			x_ranges,
			"--output",
			"target/actual-u+v+w+x-merged-ipv6-ranges.out.gz"
		};
		
		IPv6RangesMerger.main(args);
		
		File expectedFile = new File("src/test/resources/bigdata/expected-u+v+w+x-merged-ipv6-ranges.out.gz");
		File actualFile = new File("target/actual-u+v+w+x-merged-ipv6-ranges.out.gz");
		assertTrue("Result content equals expected content!", FileUtils.contentEquals(expectedFile, actualFile));
	}

	@Test
	public void i_j_k_MergeShouldReturn_expectedResultfile() throws Exception {
		String i_ranges = new File(this.getClass().getClassLoader().getResource("i.ranges.gz").getFile()).getAbsolutePath();
		String j_ranges = new File(this.getClass().getClassLoader().getResource("j.ranges.gz").getFile()).getAbsolutePath();
		String k_ranges = new File(this.getClass().getClassLoader().getResource("k.ranges.gz").getFile()).getAbsolutePath();
		String[] args = new String[] {
			"--ipv6-ranges",
			i_ranges,
			j_ranges,
		    k_ranges,
			"--output",
			"target/actual-i+j+k-merged-ranges.out.gz"
		};
		
		System.err.format("args: %s", Arrays.asList(args));
		System.err.println();
		
		IPv6RangesMerger.main(args);
		
		File expectedFile = new File("src/test/resources/expected-i+j+k-merged-ranges.out.gz");
		File actualFile = new File("target/actual-i+j+k-merged-ranges.out.gz");
		assertTrue("Result content equals expected content!", FileUtils.contentEquals(expectedFile, actualFile));
	}

}
