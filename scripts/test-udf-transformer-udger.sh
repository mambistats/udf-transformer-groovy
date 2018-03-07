#! /usr/bin/env bash

# export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
# java -version

echo $'a\tb\tc\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36\te\tf' | \
java -Xmx4G -classpath "${BASEDIR}/target/udf-transformer-groovy-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
Use Udger \
--udger-database "${BASEDIR}/resources/udgerdb_v3.dat" \
--udger-inmem \
--udger-cache 100000 \
--select "code = { ua = udger.parseUa(c[3]); c[0..2] + [ ua.ua, ua.uaClass, ua.uaEngine, ua.uaFamily, ua.uaVersionMajor, ua.uaVersion, ua.uaUptodateCurrentVersion, ua.deviceClass, ua.deviceBrand, ua.deviceMarketname, ua.os, ua.osFamily, ua.crawlerCategory ] + c[4..5] + [ 'a&b', 'c' ] }; code()" \
-D transformer.output.sep="|" \
-D transformer.array.esc.enable=true \
-D transformer.output.array.elem.sep=","
