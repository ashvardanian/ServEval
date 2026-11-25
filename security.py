"""TLS 1.3 for every Python server and client: openssl.cnf pins AES-256-GCM, X25519 and no tickets through
`OPENSSL_CONF`, which OpenSSL reads only at startup, so a TLS process re-executes itself with it set."""

import os
import pathlib
import ssl
import sys

import arguments

POLICY = pathlib.Path(__file__).resolve().with_name("openssl.cnf")


def require_tls_or_none(args, framework: str) -> None:
    """Exits unless the framework speaks the requested security; only UCall speaks Noise."""
    if args.security.startswith("noise"):
        arguments.exit(f"{framework} has no Noise; use --security none or tls")
    if args.security == "tls" and os.environ.get("OPENSSL_CONF") != str(POLICY):
        os.execve(sys.executable, [sys.executable, *sys.argv], {**os.environ, "OPENSSL_CONF": str(POLICY)})


def _files(args) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
    directory = pathlib.Path(args.data_dir) / "tls"
    files = directory / "server.pem", directory / "server.key", directory / "ca.pem"
    missing = [str(path) for path in files if not path.exists()]
    if missing:
        arguments.exit(f"{', '.join(missing)} missing; run keys.sh first")
    return files


def uvicorn_options(args) -> dict:
    """Uvicorn's TLS keywords, which Ray Serve's proxy takes too; its context reads the policy from `OPENSSL_CONF`."""
    if args.security != "tls":
        return {}
    certificate, key, _ = _files(args)
    return {"ssl_certfile": str(certificate), "ssl_keyfile": str(key)}


def client_context(args, alpn: list[str]) -> ssl.SSLContext | None:
    """The client's TLS context trusting keys.sh's CA, without resumption, or None for plaintext."""
    if args.security != "tls":
        return None
    *_, authority = _files(args)
    context = ssl.create_default_context(cafile=str(authority))
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.set_alpn_protocols(alpn)
    return context


def certificate_bytes(args) -> tuple[bytes, bytes, bytes] | None:
    """The server certificate, key and CA as bytes, for gRPC's credentials, or None for plaintext."""
    if args.security != "tls":
        return None
    return tuple(path.read_bytes() for path in _files(args))
