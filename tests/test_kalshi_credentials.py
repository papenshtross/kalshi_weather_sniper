import json

from polybot.security.kalshi_credentials import _extract_pem_and_sha, parse_kalshi_credentials, validate_kalshi_private_key_pem


def test_extract_pem_and_trailing_sha_metadata():
    pem = "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----"
    sha = "a" * 64
    out_pem, out_sha = _extract_pem_and_sha(pem + "\nsha256=" + sha)
    assert out_pem == pem
    assert out_sha == sha


def test_extract_json_wrapped_escaped_pem_and_sha():
    pem = "[REDACTED PRIVATE KEY]"
    sha = "b" * 64
    out_pem, out_sha = _extract_pem_and_sha(json.dumps({"private_key": pem, "sha": sha}))
    assert out_pem == pem
    assert out_sha == sha


def test_parse_username_key_id_password_private_key():
    pem = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----"
    creds = parse_kalshi_credentials({"username": "kid", "password": pem + "\nsha256=" + "c" * 64})
    assert creds.key_id == "kid"
    assert creds.private_key_pem == pem
    assert creds.password_sha256 == "c" * 64


def test_current_truncated_marker_is_not_valid_pem():
    ok, err = validate_kalshi_private_key_pem("-----BEGIN RSA PRIVATE KEY-----")
    assert ok is False
    assert err in {"ValueError", "UnsupportedAlgorithm"}
