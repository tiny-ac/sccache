#!/bin/bash
# Writes the certificates the REAPI TLS tests use, valid for 100 years: a CA, a server
# certificate for 127.0.0.1 and a client certificate, both signed by the CA, and another CA
# that signed neither. The keys are for the tests only, and not secret.
set -euo pipefail
cd "$(dirname "$0")"
days=36500
for ca in ca other-ca; do
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$ca.key"
    openssl req -x509 -new -key "$ca.key" -days "$days" -subj "/CN=sccache test $ca" \
        -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign" \
        -out "$ca.pem"
done
for name in server client; do
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$name.key"
    openssl req -new -key "$name.key" -subj "/CN=sccache test $name" -out "$name.csr"
done
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days "$days" \
    -out server.pem \
    -extfile <(printf 'basicConstraints=critical,CA:FALSE\nsubjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n')
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days "$days" \
    -out client.pem \
    -extfile <(printf 'basicConstraints=critical,CA:FALSE\nextendedKeyUsage=clientAuth\n')
rm -f ca.key other-ca.key ./*.csr ca.srl
