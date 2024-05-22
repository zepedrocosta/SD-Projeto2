#!/usr/bin/env bash

echo "Generating TLS credentials "

rm -f *.jks *.cert

cp cacerts client-ts.jks

generateKeyStore() {
	echo ">>>" $*
	service=$1
	pass=$2
	filename=$1
	rm -f $filename.jks

	printf "Generationg Public Key Pair: " $service " pass: " $pass

	printf "$pass\n$pass\n$service\nTP2\nSD2324\nLX\nLX\nPT\nyes\n$pass\n$pass" | keytool -ext SAN=dns:$service -genkey -alias $service -keyalg RSA -validity 365 -keystore $filename.jks -storetype pkcs12

	echo "Exporting certificate"
	printf "$pass\n" | keytool -exportcert -ext SAN=dns:$service -alias $service -keystore $filename.jks -file $filename.cert

	printf "changeit\nyes\n" | keytool -importcert -file $filename.cert -alias $service -keystore client-ts.jks
}

generateKeyStore "users0-ourorg" "tlsPassword"
generateKeyStore "shorts0-ourorg" "tlsPassword"
generateKeyStore "shorts1-ourorg" "tlsPassword"
generateKeyStore "shorts2-ourorg" "tlsPassword"
generateKeyStore "blobs0-ourorg" "tlsPassword"
generateKeyStore "blobs1-ourorg" "tlsPassword"
generateKeyStore "blobs2-ourorg" "tlsPassword"
generateKeyStore "blobs3-ourorg" "tlsPassword"