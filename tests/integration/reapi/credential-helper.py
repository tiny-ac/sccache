#!/usr/bin/env python3
# A Bazel credential helper (https://github.com/EngFlow/credential-helper-spec) for
# test-reapi-buildbarn: answers `get` with a JWT signed by the key in /jwt, valid for an hour,
# and appends each URI it gets to the file named by CREDENTIAL_HELPER_LOG.
import base64
import json
import os
import subprocess
import sys
import tempfile
import time

if sys.argv[1:] != ["get"]:
    sys.exit("usage: credential-helper.py get")
uri = json.load(sys.stdin)["uri"]
with open(os.environ["CREDENTIAL_HELPER_LOG"], "a") as log:
    log.write(uri + "\n")


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


now = int(time.time())
expires = now + 3600
header = b64url(json.dumps({"alg": "EdDSA", "typ": "JWT"}).encode())
claims = b64url(json.dumps({"aud": uri, "iat": now, "exp": expires}).encode())
# openssl signs Ed25519 only from a file. sccache runs the helper for every service at once,
# so each run has its own.
with tempfile.NamedTemporaryFile() as signing_input:
    signing_input.write(f"{header}.{claims}".encode())
    signing_input.flush()
    signature = subprocess.run(
        ["openssl", "pkeyutl", "-sign", "-inkey", "/jwt/key.pem", "-rawin", "-in", signing_input.name],
        check=True,
        capture_output=True,
    ).stdout
print(
    json.dumps(
        {
            "headers": {"Authorization": [f"Bearer {header}.{claims}.{b64url(signature)}"]},
            "expires": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires)),
        }
    )
)
