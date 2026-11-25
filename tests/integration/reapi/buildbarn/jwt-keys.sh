#!/bin/sh
# Makes the Ed25519 key that signs test-reapi-buildbarn's JWTs, and the JSON Web Key Set that
# bb-storage checks them with. bb-storage's key sets hold only public keys, so the test can't
# use an HMAC secret.
set -eu
# Compose runs this again when it starts the test after bb-storage, which reads the key set
# only every 5 minutes, so a second run keeps the key. The key set is written last, so a run
# that stopped before it makes a new key.
[ ! -f /jwt/jwks.json ] || exit 0
openssl genpkey -algorithm ed25519 -out /jwt/key.pem
# The test runs as the host's user.
chmod 644 /jwt/key.pem
python3 - <<'PY'
import base64, json, os, subprocess

der = subprocess.run(
    ['openssl', 'pkey', '-in', '/jwt/key.pem', '-pubout', '-outform', 'DER'],
    check=True, capture_output=True,
).stdout
# An Ed25519 public key's DER encoding ends with the 32 bytes of the key.
x = base64.urlsafe_b64encode(der[-32:]).rstrip(b'=').decode()
key = {'kty': 'OKP', 'crv': 'Ed25519', 'x': x, 'use': 'sig', 'alg': 'EdDSA'}
# A run that stops while writing leaves no key set, so the next run makes a new key.
with open('/jwt/jwks.json.tmp', 'w') as jwks:
    json.dump({'keys': [key]}, jwks)
os.replace('/jwt/jwks.json.tmp', '/jwt/jwks.json')
PY
