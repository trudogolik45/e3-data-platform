import time

import jwt
import pytest

from e3.identity import InvalidToken, JwksUnavailable, TokenVerifier
from tests.fakes.oidc import AUDIENCE, ISSUER, JWKS_URL, FakeOidcProvider


@pytest.fixture
def provider() -> FakeOidcProvider:
    return FakeOidcProvider()


def verifier_for(
    provider: FakeOidcProvider,
    algorithms: tuple[str, ...] = ("RS256",),
    refresh_seconds: float = 30,
) -> TokenVerifier:
    return TokenVerifier(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url=JWKS_URL,
        algorithms=algorithms,
        http=provider.http(),
        refresh_seconds=refresh_seconds,
    )


async def test_valid_token_gives_issuer_subject_and_email(provider: FakeOidcProvider) -> None:
    token = provider.token("alice", email="alice@corp.test")

    identity = await verifier_for(provider).verify(token)

    assert (identity.issuer, identity.subject) == (ISSUER, "alice")
    assert identity.email == "alice@corp.test"
    assert identity.email_verified is True


async def test_unverified_email_is_reported_as_unverified(provider: FakeOidcProvider) -> None:
    identity = await verifier_for(provider).verify(provider.token("bob", email_verified=False))

    assert identity.email_verified is False


@pytest.mark.parametrize(
    "claims",
    [
        {"iss": "https://evil.test"},
        {"aud": "another-api"},
        {"exp": int(time.time()) - 60},
        {"exp": None},
        {"sub": None},
    ],
    ids=["foreign-iss", "foreign-aud", "expired", "no-exp", "no-sub"],
)
async def test_token_with_wrong_claims_is_rejected(
    provider: FakeOidcProvider, claims: dict[str, object]
) -> None:
    subject = claims.pop("sub", "alice")
    token = provider.token(subject, **claims)  # type: ignore[arg-type]

    with pytest.raises(InvalidToken):
        await verifier_for(provider).verify(token)


async def test_unsigned_token_is_rejected(provider: FakeOidcProvider) -> None:
    token = jwt.encode(
        {"iss": ISSUER, "aud": AUDIENCE, "sub": "alice", "exp": int(time.time()) + 300},
        key="",
        algorithm="none",
    )

    with pytest.raises(InvalidToken):
        await verifier_for(provider).verify(token)


async def test_token_signed_by_another_key_is_rejected(provider: FakeOidcProvider) -> None:
    impostor = FakeOidcProvider(kid=provider.kid)

    with pytest.raises(InvalidToken):
        await verifier_for(provider).verify(impostor.token("alice"))


async def test_algorithm_outside_allow_list_is_rejected(provider: FakeOidcProvider) -> None:
    with pytest.raises(InvalidToken):
        await verifier_for(provider, algorithms=("ES256",)).verify(provider.token("alice"))


async def test_garbage_is_rejected(provider: FakeOidcProvider) -> None:
    with pytest.raises(InvalidToken):
        await verifier_for(provider).verify("not-a-jwt")


async def test_jwks_is_fetched_once_for_many_tokens(provider: FakeOidcProvider) -> None:
    verifier = verifier_for(provider)

    await verifier.verify(provider.token("alice"))
    await verifier.verify(provider.token("bob"))

    assert provider.jwks_requests == 1


async def test_unknown_key_id_refetches_jwks_after_key_rotation(
    provider: FakeOidcProvider,
) -> None:
    verifier = verifier_for(provider, refresh_seconds=0)
    await verifier.verify(provider.token("alice"))
    provider.kid = "rotated-key"

    identity = await verifier.verify(provider.token("alice"))

    assert identity.subject == "alice"
    assert provider.jwks_requests == 2


async def test_unknown_key_id_does_not_hammer_jwks(provider: FakeOidcProvider) -> None:
    verifier = verifier_for(provider)
    await verifier.verify(provider.token("alice"))
    provider.kid = "rotated-key"

    for _ in range(3):
        with pytest.raises(InvalidToken):
            await verifier.verify(provider.token("alice"))

    assert provider.jwks_requests == 1


async def test_unreachable_jwks_is_not_an_invalid_token(provider: FakeOidcProvider) -> None:
    verifier = TokenVerifier(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url=f"{ISSUER}/missing",
        algorithms=("RS256",),
        http=provider.http(),
    )

    with pytest.raises(JwksUnavailable):
        await verifier.verify(provider.token("alice"))


@pytest.mark.parametrize("algorithm", ["none", "HS256"])
def test_symmetric_and_none_algorithms_cannot_be_allowed(algorithm: str) -> None:
    with pytest.raises(ValueError, match="алгоритм"):
        verifier_for(FakeOidcProvider(), algorithms=(algorithm,))
