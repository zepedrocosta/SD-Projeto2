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
	
	echo "$pass\n$pass\n$service\nTP2\nSD2324\nLX\nLX\nPT\nyes\n$pass\n$pass" | keytool -ext SAN=dns:$service -genkey -alias $service -keyalg RSA -validity 365 -keystore $filename.jks -storetype pkcs12

	echo "Exporting certificate"
	echo "$pass\n" | keytool -exportcert -ext SAN=dns:$service -alias $service -keystore $filename.jks -file $filename.cert
	
	echo "changeit\nyes\n" | keytool -importcert  -file $filename.cert -alias $service -keystore client-ts.jks
}

generateKeyStore "users" "123users"
