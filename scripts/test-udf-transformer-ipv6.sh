#! /usr/bin/env bash

echo $'a\tb\tc\t127.126.125.124\t223.123.124.129\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\tq\tr' | \
java -Xmx4G -cp "${BASEDIR}/target/udf-transformer-groovy-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
Use Common,IPv6 \
--ipv6-ranges "${BASEDIR}/resources/ipv6+4_network_ranges.gz" \
--select "[ c.col(1), c.col(2), c.col(3), c.col(4), c.col(5), ipv6.ip(c.col(4)), ipv6.ip(c.col(5)), c.col(6), c.col(7), c.col(8), c.col(9), c.col(10), c.col(11), c.col(12), c.col(13), c.col(14), c.col(15), c.col(16), c.col(17), c.concat(c.col(1), c.col(2)), c.concat_v(c.col(6), c.col(7), c.col(8)) ]"
