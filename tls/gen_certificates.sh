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

	echo "Generationg Public Key Pair: " $service " pass: " $pass

	printf "$pass\n$pass\n$service\nTP2\nSD2324\nLX\nLX\nPT\nyes\n$pass\n$pass" | keytool -ext SAN=dns:$service -genkey -alias $service -keyalg RSA -validity 365 -keystore $filename.jks -storetype pkcs12

	echo "Exporting certificate"
	printf "$pass\n" | keytool -exportcert -ext SAN=dns:$service -alias $service -keystore $filename.jks -file $filename.cert

	printf "changeit\nyes\n" | keytool -importcert -file $filename.cert -alias $service -keystore client-ts.jks
}

generateKeyStore "users0-0" "users0-0pwd"
generateKeyStore "shorts0-0" "shorts0-0pwd"
generateKeyStore "shorts1-0" "shorts1-0pwd"
generateKeyStore "shorts2-0" "shorts2-0pwd"
generateKeyStore "blobs0-0" "blobs0-0pwd"
generateKeyStore "blobs1-0" "blobs1-0pwd"
generateKeyStore "blobs2-0" "blobs2-0pwd"
generateKeyStore "blobs3-0" "blobs3-0pwd"