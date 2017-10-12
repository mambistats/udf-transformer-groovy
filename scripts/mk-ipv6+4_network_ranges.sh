#! /usr/bin/env bash
if [ "x$(uname -s)" = "xDarwin" ]; then
  GEOIP2_CSV_CONVERTER_CMD="${BASEDIR}/scripts/geoip2-csv-converter-darwin"
else
  GEOIP2_CSV_CONVERTER_CMD="${BASEDIR}/scripts/geoip2-csv-converter-linux"
fi
echo "using [${GEOIP2_CSV_CONVERTER_CMD}]"
mkdir -p "${BASEDIR}/resources"
cd "${BASEDIR}/resources"
curl -o GeoLite2-City-CSV.zip http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip
unzip GeoLite2-City-CSV.zip
cd GeoLite2-City-CSV_????????
echo "Adding ranges ..."
"${GEOIP2_CSV_CONVERTER_CMD}" -include-range -block-file="GeoLite2-City-Blocks-IPv4.csv" -output-file="GeoLite2-City-Blocks-IPv4-with-ranges.csv"
"${GEOIP2_CSV_CONVERTER_CMD}" -include-range -block-file="GeoLite2-City-Blocks-IPv6.csv" -output-file="GeoLite2-City-Blocks-IPv6-with-ranges.csv"
echo "Creating combined City Blocks IPv6+4 with header stripped ..."
tail -n +2 "GeoLite2-City-Blocks-IPv4-with-ranges.csv" > "GeoLite2-City-Blocks-IPv6+4-with-ranges.csv"
tail -n +2 "GeoLite2-City-Blocks-IPv6-with-ranges.csv" >> "GeoLite2-City-Blocks-IPv6+4-with-ranges.csv"
echo "Extracting ranges and creating range file ..."
cat GeoLite2-City-Blocks-IPv6+4-with-ranges.csv | python -c "import sys; [sys.stdout.write(line.split(',')[0] + '\t' + line.split(',')[1] + '\n') for line in sys.stdin]" | gzip -c > ../ipv6+4_network_ranges.gz
echo "Complete!"
