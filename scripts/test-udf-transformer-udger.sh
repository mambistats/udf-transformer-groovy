#! /usr/bin/env bash

# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
# java -version

echo $'a\tb\tc\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36\te\tf' | \
java -Xmx4G -cp "${BASEDIR}/target/udf-transformer-groovy-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
Use Udger \
--udger-database "${BASEDIR}/resources/udgerdb_v3.dat" \
--udger-inmem \
--udger-cache 100000 \
--select "[ c[0], c[1], c[2], udger.parseUa(c[3]), c[4], c[5] ]" \
--output-sep "|"
