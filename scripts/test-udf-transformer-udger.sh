#! /usr/bin/env bash

# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
# java -version

echo $'a\tb\tc\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36\te\tf' | \
java -Xmx4G -cp "${BASEDIR}/target/udf-transformer-groovy-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
Use Common,Udger \
--udger-database "${BASEDIR}/resources/udgerdb_v3.dat" \
--udger-inmem \
--udger-cache 100000 \
--select "[ c.col(1), c.col(2), c.col(3), udger.parseUa('Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'), c.col(5), c.col(6) ]" \
--output-sep "|"