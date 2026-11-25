#!/bin/sh
# Writes a P-256 CA, a server leaf naming every SERVEVAL_INTERFACES address, and Noise keys under SERVEVAL_DATA_DIR.
set -eu
directory="${SERVEVAL_DATA_DIR:-data}"
names=""
for interface in $(printf '%s' "${SERVEVAL_INTERFACES:-127.0.0.1}" | tr ',' ' '); do
    for address in $(printf '%s' "${interface#*=}" | tr '+' ' '); do
        names="$names,IP:$address"
    done
done
mkdir -p "$directory/tls" "$directory/noise"
cd "$directory/tls"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 365 -subj "/CN=ServEval CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign" \
    -keyout ca.key -out ca.pem 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=ServEval" \
    -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n' "${names#,}" > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 365 -extfile server.ext \
    -out server.pem 2>/dev/null
rm -f server.csr server.ext ca.srl
echo "TLS: $directory/tls/{ca.pem,server.pem,server.key} for ${names#,}"
cd - >/dev/null
if ! command -v ucall >/dev/null; then
    echo "Noise: skipped, the ucall tool from a UCall build is not on PATH"
    exit 0
fi
for role in server client; do
    rm -f "$directory/noise/$role.key"
    ucall keygen "$directory/noise/$role.key" > "$directory/noise/$role.pub"
done
echo "Noise: $directory/noise/{server,client}.{key,pub}"
