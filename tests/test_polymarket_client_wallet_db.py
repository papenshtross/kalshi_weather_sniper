from polybot.adapters.polymarket.client import _config_from_wallet_row


def test_config_from_wallet_row_prefers_dashboard_wallet_values():
    row = {
        "private_key_encrypted": "0xabc",
        "private_key": None,
        "proxy_address": "0xproxy",
        "signature_type": 1,
        "clob_api_key": "key",
        "clob_api_secret": "secret",
        "clob_api_passphrase": "pass",
    }

    cfg = _config_from_wallet_row(row)

    assert cfg["private_key"] == "0xabc"
    assert cfg["proxy_address"] == "0xproxy"
    assert cfg["signature_type"] == 1
    assert cfg["api_key"] == "key"
    assert cfg["api_secret"] == "secret"
    assert cfg["api_passphrase"] == "pass"
