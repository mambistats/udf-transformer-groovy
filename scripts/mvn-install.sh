#! /usr/bin/env bash
source "${HOME}/.bashrc"
mvn install:install-file \
  -Dfile="${BASEDIR}/target/udf-transformer-groovy-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
  -DgroupId="com.kc14.hadoop.hive" \
  -DartifactId="udf-transformer-groovy" \
  -Dversion="0.0.1" \
  -Dpackaging="jar"
