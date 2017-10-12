#! /usr/bin/env bash
cd "${BASEDIR}/resources"
curl -L -H "Accept: application/octet-stream" "https://github.com/udger/test-data/raw/master/data_v3/udgerdb_v3.dat" -o udgerdb_v3.dat
echo 'Done'
