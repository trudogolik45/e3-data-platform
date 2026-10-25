"""Fake OIDC-провайдер: свой RSA-ключ, JWKS через `httpx.MockTransport` и выпуск JWT."""

import time
from typing import Any

import httpx
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

ISSUER = "https://idp.test/e3"
AUDIENCE = "e3-api"
JWKS_URL = f"{ISSUER}/jwks"


class FakeOidcProvider:
    def __init__(self, kid: str = "test-key") -> None:
        self.kid = kid
        self._key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.jwks_requests = 0

    def jwks(self) -> dict[str, Any]:
        jwk: dict[str, Any] = RSAAlgorithm.to_jwk(self._key.public_key(), as_dict=True)
        return {"keys": [{**jwk, "kid": self.kid, "alg": "RS256", "use": "sig"}]}

    def http(self) -> httpx.AsyncClient:
        """Клиент, которому JWKS отдаёт этот провайдер."""

        def handle(request: httpx.Request) -> httpx.Response:
            if str(request.url) != JWKS_URL:
                return httpx.Response(404)
            self.jwks_requests += 1
            return httpx.Response(200, json=self.jwks())

        return httpx.AsyncClient(transport=httpx.MockTransport(handle))

    def token(
        self,
        subject: str,
        *,
        email: str | None = None,
        email_verified: bool = True,
        headers: dict[str, Any] | None = None,
        **claims: Any,
    ) -> str:
        now = int(time.time())
        payload: dict[str, Any] = {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": subject,
            "iat": now,
            "exp": now + 300,
            "email": email or f"{subject}@e3.test",
            "email_verified": email_verified,
            **claims,
        }
        payload = {name: value for name, value in payload.items() if value is not None}
        header = {"kid": self.kid, **(headers or {})}
        header = {name: value for name, value in header.items() if value is not None}
        return jwt.encode(payload, self._key, algorithm="RS256", headers=header)
