"""Tests for sysap/rest.py: RestClient, SSL modes, adaptive concurrency (docs/01 §2; docs/06)."""

import asyncio
import ssl
from pathlib import Path

import pytest

from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.sysap.rest import (
    AdaptiveLimiter,
    AuthenticationError,
    BadRequestError,
    CommandFailedError,
    ForbiddenError,
    NotFoundError,
    RestClient,
    ServerOverloadedError,
    SysApError,
    SysApServerError,
    build_ssl_context,
)

SERIAL = "ABB7F500E17A"

SAMPLE_CONFIG = {
    "sysapName": "Test House",
    "sysap": {},
    "users": {},
    "floorplan": {"floors": {}},
    "devices": {
        SERIAL: {
            "displayName": "Ceiling Light",
            "channels": {
                "ch0003": {
                    "displayName": "Switch",
                    "functionID": "7",
                    "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                    "outputs": {"odp0000": {"pairingID": 256, "value": "0"}},
                }
            },
        }
    },
}

# A throwaway, real self-signed CA cert -- not a secret, just PEM content for the ssl module.
TEST_CA_PEM = """-----BEGIN CERTIFICATE-----
MIIDJTCCAg2gAwIBAgIUSxtfQifj6O0QH+FnXi7ZoYk1g6UwDQYJKoZIhvcNAQEL
BQAwIjEgMB4GA1UEAwwXZnJlZWF0aG9tZTJtcXR0LXRlc3QtY2EwHhcNMjYwODIz
MjA1NjQwWhcNMzYwODIwMjA1NjQwWjAiMSAwHgYDVQQDDBdmcmVlYXRob21lMm1x
dHQtdGVzdC1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKkW9Ic0
ekbdSwJrTll6NyfxnjGGl/DiHhavIaqSRSa8ksj4SMB8c52Z61yckDPVzJWgEgT3
Uv5+Um29hua4v3YuOO1N8voGA4AtSeVSRQNi5ltLDDklp6+fTvARGCc+eg/TX8JK
JgSv7c03bkrnqWgjLkOJ0wdu8ERFgvYEvBKrTzmAkhC4DNbTzt2YVuBBdhCDP8ue
g1HVMrCTU4excqcQotouvSKsqaN4zOhsQ8xX/qy6apXqKD0gfX6Fo1TIifdbpM9z
CGEMan5e9YlR06TLO4r+qEdAvEfW21uAzMVO3f5LxrGH+o81PjrDV48Q0X4sHyog
+THxURP0aV3j4IMCAwEAAaNTMFEwHQYDVR0OBBYEFI1pQVZakD38JljCIhhLOJfI
OsxTMB8GA1UdIwQYMBaAFI1pQVZakD38JljCIhhLOJfIOsxTMA8GA1UdEwEB/wQF
MAMBAf8wDQYJKoZIhvcNAQELBQADggEBAKROcIXMqnGlWCGmHzaIHuxoR8jtuMCx
58WNH4IQt9p8lqgdHzZXt8e33do+v9VkIClPof60yVxqXo2K1DY/ZpZ288SmwSNf
o2XXimM6X+kSrJtVEfoHTX7A5GrKqVoa+uiN4SGYIyXgFEvCfGZIAsD2QOan3NnI
rX54+Ss0Cm6Qq4S5xmZ+7TocwQgZgjs9Awsj7Gi9lfiDwcPLBf81o99hlb07gr6s
taEUX3zOBTydv7ZlxX+vMziBd3bKNDnfXyYng0losfGxsW0bRZ10e+g+UoWNaLel
6wFlSoTnvo6DsnmNSyJnvnAMZdknEMfqaBpB8W4yap1Bn5spDfyn6oY=
-----END CERTIFICATE-----
"""


def _client_for(client, **kwargs) -> RestClient:
    base_url = str(client.make_url("")).rstrip("/")
    return RestClient(
        base_url=base_url,
        username="installer",
        password="secret",
        session=client.session,
        **kwargs,
    )


# ------------------------------------------------------------------------ build_ssl_context


async def test_build_ssl_context_off_disables_verification() -> None:
    assert await build_ssl_context("off") is False


async def test_build_ssl_context_verify_returns_default_context() -> None:
    result = await build_ssl_context("verify")
    assert isinstance(result, ssl.SSLContext)


async def test_build_ssl_context_ca_file_returns_context_with_ca_loaded(tmp_path: Path) -> None:
    ca_path = tmp_path / "ca.pem"
    ca_path.write_text(TEST_CA_PEM)
    result = await build_ssl_context("ca_file", ca_file=ca_path)
    assert isinstance(result, ssl.SSLContext)


async def test_build_ssl_context_ca_file_requires_ca_file() -> None:
    with pytest.raises(ValueError):
        await build_ssl_context("ca_file")


async def test_build_ssl_context_rejects_unknown_mode() -> None:
    with pytest.raises(ValueError):
        await build_ssl_context("bogus")  # type: ignore[arg-type]


# ------------------------------------------------------------------------- AdaptiveLimiter


async def test_adaptive_limiter_halves_on_overload_with_floor_of_one() -> None:
    limiter = AdaptiveLimiter(4)
    await limiter.record_overload()
    assert limiter.limit == 2
    await limiter.record_overload()
    assert limiter.limit == 1
    await limiter.record_overload()
    assert limiter.limit == 1


async def test_adaptive_limiter_recovers_by_one_per_success_up_to_max() -> None:
    limiter = AdaptiveLimiter(4)
    await limiter.record_overload()
    assert limiter.limit == 2
    await limiter.record_success()
    assert limiter.limit == 3
    await limiter.record_success()
    assert limiter.limit == 4
    await limiter.record_success()
    assert limiter.limit == 4


async def test_adaptive_limiter_never_exceeds_its_limit() -> None:
    limiter = AdaptiveLimiter(2)
    concurrent = 0
    peak = 0

    async def worker() -> None:
        nonlocal concurrent, peak
        async with limiter:
            concurrent += 1
            peak = max(peak, concurrent)
            await asyncio.sleep(0.02)
            concurrent -= 1

    await asyncio.gather(*(worker() for _ in range(6)))
    assert peak <= 2


# --------------------------------------------------------------------------- RestClient: happy path


async def test_get_configuration_resolves_sysap_uuid() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        rest = _client_for(client)
        config = await rest.get_configuration()
        assert rest.sysap_uuid == fake.sysap_uuid
        assert config["devices"][SERIAL]["displayName"] == "Ceiling Light"


async def test_non_default_sysap_uuid_is_resolved_from_configuration() -> None:
    custom_uuid = "de305d54-75b4-431b-adb2-eb6b9e546014"
    fake = FakeSysAp(sysap_uuid=custom_uuid, configuration=SAMPLE_CONFIG)
    async with running_fake_sysap(fake) as (fake, client):
        rest = _client_for(client)
        await rest.get_configuration()
        assert rest.sysap_uuid == custom_uuid


async def test_get_and_put_datapoint_roundtrip() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (_fake, client):
        rest = _client_for(client)
        await rest.get_configuration()
        await rest.put_datapoint(f"{SERIAL}.ch0003.idp0000", "1")
        value = await rest.get_datapoint(f"{SERIAL}.ch0003.idp0000")
        assert value == "1"


# --------------------------------------------------------------------------- typed errors, no retry


async def test_non_ok_result_is_a_failure() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        rest = _client_for(client)
        await rest.get_configuration()
        fake.set_write_result("FAILED")
        with pytest.raises(CommandFailedError):
            await rest.put_datapoint(f"{SERIAL}.ch0003.idp0000", "1")


async def test_auth_failure_is_not_retried() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        fake.set_error("/fhapi/v1/api/rest/configuration", 401)
        rest = _client_for(client, max_attempts=5)
        with pytest.raises(AuthenticationError):
            await rest.get_configuration()
        assert fake.request_count("/fhapi/v1/api/rest/configuration") == 1


@pytest.mark.parametrize(
    ("status", "exc_type"), [(400, BadRequestError), (403, ForbiddenError), (404, NotFoundError)]
)
async def test_client_errors_are_not_retried(status: int, exc_type: type[Exception]) -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        fake.set_error("/fhapi/v1/api/rest/configuration", status)
        rest = _client_for(client, max_attempts=5)
        with pytest.raises(exc_type):
            await rest.get_configuration()
        assert fake.request_count("/fhapi/v1/api/rest/configuration") == 1


# --------------------------------------------------------------------- 502 and adaptive concurrency


async def test_sysap_502_reduces_concurrency() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        rest = _client_for(client, max_inflight=4, max_attempts=1)

        fake.set_error("/fhapi/v1/api/rest/configuration", 502)
        with pytest.raises(ServerOverloadedError):
            await rest.get_configuration()
        assert rest.concurrency_limit == 2

        fake.clear_error("/fhapi/v1/api/rest/configuration")
        await rest.get_configuration()
        assert rest.concurrency_limit == 3
        await rest.get_configuration()
        assert rest.concurrency_limit == 4
        await rest.get_configuration()
        assert rest.concurrency_limit == 4


async def test_other_5xx_is_retried_without_reducing_concurrency() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        rest = _client_for(client, max_inflight=4, max_attempts=1)
        fake.set_error("/fhapi/v1/api/rest/configuration", 500)
        with pytest.raises(SysApServerError):
            await rest.get_configuration()
        assert rest.concurrency_limit == 4


async def test_get_datapoint_before_configuration_raises() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (_fake, client):
        rest = _client_for(client)
        with pytest.raises(Exception, match="sysap_uuid is not resolved"):
            await rest.get_datapoint(f"{SERIAL}.ch0003.odp0000")


async def test_get_datapoint_rejects_malformed_response() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        rest = _client_for(client)
        await rest.get_configuration()
        path = f"/fhapi/v1/api/rest/datapoint/{fake.sysap_uuid}/{SERIAL}.ch0003.odp0000"
        fake.set_raw_response(path, {fake.sysap_uuid: {"unexpected": True}})
        with pytest.raises(CommandFailedError):
            await rest.get_datapoint(f"{SERIAL}.ch0003.odp0000")


async def test_unwrap_rejects_body_with_more_than_one_key() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        rest = _client_for(client)
        await rest.get_configuration()
        path = "/fhapi/v1/api/rest/configuration"
        fake.set_raw_response(path, {fake.sysap_uuid: {}, "another-uuid": {}})
        with pytest.raises(SysApError):
            await rest.get_configuration()


async def test_unwrap_warns_on_mismatched_sysap_uuid_but_still_returns_payload(caplog) -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        rest = _client_for(client)
        await rest.get_configuration()
        path = "/fhapi/v1/api/rest/configuration"
        fake.set_raw_response(path, {"11111111-1111-1111-1111-111111111111": {"devices": {}}})
        with caplog.at_level("WARNING"):
            config = await rest.get_configuration()
        assert config == {"devices": {}}
        assert "unexpected SysAP UUID" in caplog.text


async def test_502_retried_then_succeeds_within_max_attempts() -> None:
    async with running_fake_sysap(FakeSysAp(configuration=SAMPLE_CONFIG)) as (fake, client):
        rest = _client_for(client, max_attempts=3, backoff_initial=0.01, backoff_cap=0.02)
        fake.set_error("/fhapi/v1/api/rest/configuration", 502, times=1)

        config = await rest.get_configuration()

        assert config["devices"]
        assert fake.request_count("/fhapi/v1/api/rest/configuration") == 2
        # the one 502 halved it (4 -> 2), then the following success recovered it by one (2 -> 3)
        assert rest.concurrency_limit == 3
