#! /usr/bin/env bash

echo $'a\tb\tc\t127.126.125.124\t223.123.124.129\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\tq\tr' | \
java -Xmx4G -cp "${BASEDIR}/target/udf-transformer-groovy-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
Use Common,IPv6 \
--ipv6-ranges "${BASEDIR}/resources/ipv6+4_network_ranges.gz" \
--select "[ c[0], c[1], c[2], c[3], c[4], ipv6.ip(c[3]), ipv6.ip(c[4]), c[5], c[6], c[7], c[8], c[9], c[10], c[11], c[12], c[13], c[14], c[15], c[16], common.concat(c[0], c[1]), common.concat_v(c[5], c[6], c[7]) ]"
