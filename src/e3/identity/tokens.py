"""Проверка OIDC JWT: подпись по JWKS провайдера, `iss`, `aud`, `exp` и allow-list алгоритмов.

Токен говорит только, кто пользователь (`iss` + `sub`); права берутся из Membership (ADR-0004).
"""

import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import httpx
import jwt

ASYMMETRIC_PREFIXES = ("RS", "PS", "ES", "EdDSA")
# Неизвестный `kid` перезапрашивает JWKS не чаще раза в это время: иначе мусорные
# токены превращаются в поток запросов к провайдеру.
JWKS_REFRESH_SECONDS = 30.0


class InvalidToken(Exception):
    """Токен не прошёл проверку: текст — причина для лога, не для пользователя."""


class JwksUnavailable(Exception):
    """Ключи провайдера не получить: проверить токен сейчас нельзя."""


@dataclass(frozen=True)
class Identity:
    issuer: str
    subject: str
    email: str | None
    email_verified: bool


class TokenVerifier:
    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        jwks_url: str,
        algorithms: Sequence[str],
        http: httpx.AsyncClient,
        refresh_seconds: float = JWKS_REFRESH_SECONDS,
    ) -> None:
        for algorithm in algorithms:
            if not algorithm.startswith(ASYMMETRIC_PREFIXES):
                raise ValueError(f"алгоритм {algorithm} не разрешён: только асимметричные")
        self._issuer = issuer
        self._audience = audience
        self._jwks_url = jwks_url
        self._algorithms = list(algorithms)
        self._http = http
        self._refresh_seconds = refresh_seconds
        self._keys: dict[str, jwt.PyJWK] = {}
        self._fetched_at: float | None = None

    async def verify(self, token: str) -> Identity:
        key = await self._signing_key(_header(token))
        try:
            claims: dict[str, Any] = jwt.decode(
                token,
                key,
                algorithms=self._algorithms,
                audience=self._audience,
                issuer=self._issuer,
                options={"require": ["exp", "iss", "aud", "sub"]},
            )
        except jwt.PyJWTError as error:
            raise InvalidToken(str(error)) from None
        return Identity(
            issuer=claims["iss"],
            subject=claims["sub"],
            email=claims.get("email"),
            email_verified=claims.get("email_verified") is True,
        )

    async def _signing_key(self, header: dict[str, Any]) -> jwt.PyJWK:
        if header.get("alg") not in self._algorithms:
            raise InvalidToken(f"алгоритм {header.get('alg')} вне allow-list")
        kid = header.get("kid")
        if not isinstance(kid, str):
            raise InvalidToken("в заголовке нет kid")
        if kid not in self._keys and self._may_refresh():
            await self._refresh()
        if kid not in self._keys:
            raise InvalidToken(f"неизвестный kid {kid}")
        return self._keys[kid]

    def _may_refresh(self) -> bool:
        if self._fetched_at is None:
            return True
        return time.monotonic() - self._fetched_at >= self._refresh_seconds

    async def _refresh(self) -> None:
        try:
            response = await self._http.get(self._jwks_url)
            response.raise_for_status()
            jwks = jwt.PyJWKSet.from_dict(response.json())
        except (httpx.HTTPError, ValueError, jwt.PyJWTError) as error:
            raise JwksUnavailable(str(error)) from error
        self._keys = {key.key_id: key for key in jwks.keys if key.key_id}
        self._fetched_at = time.monotonic()


def _header(token: str) -> dict[str, Any]:
    try:
        return jwt.get_unverified_header(token)
    except jwt.PyJWTError as error:
        raise InvalidToken(str(error)) from None
