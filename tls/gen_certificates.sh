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

generateKeyStore "users0-ourorg" "users0-0pwd"
generateKeyStore "shorts0-ourorg" "shorts0-0pwd"
generateKeyStore "shorts1-ourorg" "shorts1-0pwd"
generateKeyStore "shorts2-ourorg" "shorts2-0pwd"
generateKeyStore "blobs0-ourorg" "blobs0-0pwd"
generateKeyStore "blobs1-ourorg" "blobs1-0pwd"
generateKeyStore "blobs2-ourorg" "blobs2-0pwd"
generateKeyStore "blobs3-ourorg" "blobs3-0pwd"