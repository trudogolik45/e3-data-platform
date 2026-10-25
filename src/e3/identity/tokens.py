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
# Даже без новых `kid` ключи перечитываются: отозванный провайдером ключ не живёт вечно.
JWKS_MAX_AGE_SECONDS = 3600.0


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
        max_age_seconds: float = JWKS_MAX_AGE_SECONDS,
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
        self._max_age_seconds = max_age_seconds
        self._keys: list[jwt.PyJWK] = []
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
        if kid is not None and not isinstance(kid, str):
            raise InvalidToken("kid не строка")
        if self._age() >= self._max_age_seconds or (
            self._find(kid) is None and self._age() >= self._refresh_seconds
        ):
            await self._refresh()
        key = self._find(kid)
        if key is None:
            raise InvalidToken(f"нет ключа для kid {kid}")
        return key

    def _find(self, kid: str | None) -> jwt.PyJWK | None:
        """Без `kid` подходит только единственный ключ провайдера."""
        if kid is None:
            return self._keys[0] if len(self._keys) == 1 else None
        return next((key for key in self._keys if key.key_id == kid), None)

    def _age(self) -> float:
        if self._fetched_at is None:
            return float("inf")
        return time.monotonic() - self._fetched_at

    async def _refresh(self) -> None:
        try:
            response = await self._http.get(self._jwks_url)
            response.raise_for_status()
            jwks = jwt.PyJWKSet.from_dict(response.json())
        except (httpx.HTTPError, ValueError, jwt.PyJWTError) as error:
            raise JwksUnavailable(str(error)) from error
        self._keys = list(jwks.keys)
        self._fetched_at = time.monotonic()


def _header(token: str) -> dict[str, Any]:
    try:
        return jwt.get_unverified_header(token)
    except jwt.PyJWTError as error:
        raise InvalidToken(str(error)) from None
