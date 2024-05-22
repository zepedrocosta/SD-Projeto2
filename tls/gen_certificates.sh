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

	
    echo "Generating Public Key Pair: $service, pass: $pass"

    # Use a heredoc to provide input to keytool
    keytool -genkey -alias $service -keyalg RSA -validity 365 -keystore $filename.jks -storetype pkcs12 -dname "CN=$service, OU=., O=., L=., S=., C=." -ext SAN=dns:$service -storepass $pass -keypass $pass

    echo "Exporting certificate"
    keytool -exportcert -ext SAN=dns:$service -alias $service -keystore $filename.jks -file $filename.cert -storepass $pass

    echo "Importing certificate into client-ts.jks"
    keytool -importcert -file $filename.cert -alias $service -keystore client-ts.jks -storepass changeit -noprompt

}

# List of services
services=(
    "users0-ourorg"
    "shorts0-ourorg"
    "shorts1-ourorg"
    "shorts2-ourorg"
    "blobs0-ourorg"
    "blobs1-ourorg"
    "blobs2-ourorg"
    "blobs3-ourorg"
)

# Password for keystore
password="tlsPassword"

# Generate certificates for all services
for service in "${services[@]}"; do
    generateKeyStore "$service" "$password"
done