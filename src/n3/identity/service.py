"""User, Organization, Project и Membership в PostgreSQL Control Plane (ADR-0004).

Изменения Membership одного Project идут под блокировкой его строки: проверка
«последний Admin» и само изменение — в одной транзакции, гонок нет.
"""

from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from n3.identity.access import Principal, Role
from n3.identity.models import MembershipRow, OrganizationRow, ProjectRow, UserRow
from n3.identity.tokens import Identity

# Техническая идентичность запусков, созданных до появления Initiator. Войти ею
# нельзя, Membership у неё нет (миграция 0010).
LEGACY_MIGRATION_USER_ID = UUID("00000000-0000-4000-8000-0000000000ff")


class UserNotFound(Exception):
    """Нет такого User или подтверждённого email."""


class UserAmbiguous(Exception):
    """Подтверждённый email есть у нескольких User: нужен `user_id`."""


class OrganizationNotFound(Exception):
    pass


class ProjectNotFound(Exception):
    pass


class MemberExists(Exception):
    """User уже участник Project."""


class MemberNotFound(Exception):
    """User не участник Project."""


class LastAdmin(Exception):
    """Изменение оставило бы Project без Admin."""


@dataclass(frozen=True)
class Member:
    user_id: UUID
    email: str | None
    role: Role


class IdentityService:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def sign_in(self, identity: Identity) -> UUID:
        """User по `iss` + `sub`: создаётся при первом входе, email обновляется при каждом.

        Неподтверждённый email не хранится: им нельзя ни искать, ни показывать.
        """
        email = identity.email if identity.email_verified else None
        async with self._sessions.begin() as session:
            user_id = await session.scalar(
                insert(UserRow)
                .values(
                    id=uuid4(),
                    issuer=identity.issuer,
                    subject=identity.subject,
                    email=email,
                    email_verified=identity.email_verified,
                )
                .on_conflict_do_update(
                    index_elements=["issuer", "subject"],
                    set_={"email": email, "email_verified": identity.email_verified},
                )
                .returning(UserRow.id)
            )
        if user_id is None:
            raise RuntimeError(f"User {identity.issuer} {identity.subject} не сохранился")
        return user_id

    async def principal(self, user_id: UUID) -> Principal:
        async with self._sessions() as session:
            rows = await session.scalars(
                select(MembershipRow).where(MembershipRow.user_id == user_id)
            )
            return Principal(user_id, {row.project_id: Role(row.role) for row in rows})

    async def role_of(self, user_id: UUID, project_id: UUID) -> Role | None:
        async with self._sessions() as session:
            row = await session.get(MembershipRow, (project_id, user_id))
            return Role(row.role) if row else None

    async def has_role(self, user_id: UUID, project_id: UUID, minimum: Role) -> bool:
        role = await self.role_of(user_id, project_id)
        return role is not None and role.includes(minimum)

    async def member(self, project_id: UUID, user_id: UUID) -> Member:
        async with self._sessions() as session:
            row = await session.get(MembershipRow, (project_id, user_id))
            if row is None:
                raise MemberNotFound(str(user_id))
            user = await session.get_one(UserRow, user_id)
            return Member(user_id, user.email, Role(row.role))

    async def find_user(self, *, user_id: UUID | None = None, email: str | None = None) -> UUID:
        async with self._sessions() as session:
            if user_id is not None:
                if await session.get(UserRow, user_id) is None:
                    raise UserNotFound(str(user_id))
                return user_id
            found = list(
                await session.scalars(
                    select(UserRow.id).where(UserRow.email == email, UserRow.email_verified)
                )
            )
        if not found:
            raise UserNotFound(str(email))
        if len(found) > 1:
            raise UserAmbiguous(str(email))
        return found[0]

    async def create_organization(self, name: str) -> UUID:
        organization_id = uuid4()
        async with self._sessions.begin() as session:
            session.add(OrganizationRow(id=organization_id, name=name))
        return organization_id

    async def create_project(self, organization_id: UUID, name: str, *, admin: UUID) -> UUID:
        project_id = uuid4()
        async with self._sessions.begin() as session:
            if await session.get(OrganizationRow, organization_id) is None:
                raise OrganizationNotFound(str(organization_id))
            if await session.get(UserRow, admin) is None:
                raise UserNotFound(str(admin))
            session.add(ProjectRow(id=project_id, organization_id=organization_id, name=name))
            await session.flush()
            session.add(MembershipRow(project_id=project_id, user_id=admin, role=Role.ADMIN))
        return project_id

    async def members(self, project_id: UUID) -> list[Member]:
        async with self._sessions() as session:
            rows = await session.execute(
                select(MembershipRow, UserRow.email)
                .join(UserRow, UserRow.id == MembershipRow.user_id)
                .where(MembershipRow.project_id == project_id)
                .order_by(UserRow.created_at)
            )
            return [Member(row.user_id, email, Role(row.role)) for row, email in rows]

    async def add_member(self, project_id: UUID, user_id: UUID, role: Role) -> None:
        async with self._sessions.begin() as session:
            await _lock_project(session, project_id)
            if await session.get(UserRow, user_id) is None:
                raise UserNotFound(str(user_id))
            if await session.get(MembershipRow, (project_id, user_id)) is not None:
                raise MemberExists(str(user_id))
            session.add(MembershipRow(project_id=project_id, user_id=user_id, role=role))
            try:
                await session.flush()
            except IntegrityError:
                raise MemberExists(str(user_id)) from None

    async def change_role(self, project_id: UUID, user_id: UUID, role: Role) -> None:
        async with self._sessions.begin() as session:
            member = await _locked_member(session, project_id, user_id)
            if role != Role.ADMIN:
                await _keep_an_admin(session, member)
            member.role = role

    async def remove_member(self, project_id: UUID, user_id: UUID) -> None:
        async with self._sessions.begin() as session:
            member = await _locked_member(session, project_id, user_id)
            await _keep_an_admin(session, member)
            await session.delete(member)

    async def ensure_dev(self, *, user_id: UUID, organization_id: UUID, project_id: UUID) -> None:
        """dev-режим: один Organization, один Project, его Admin — идемпотентно."""
        async with self._sessions.begin() as session:
            for row in (
                UserRow(id=user_id, issuer="urn:n3:dev", subject="dev", email_verified=False),
                OrganizationRow(id=organization_id, name="dev"),
                ProjectRow(id=project_id, organization_id=organization_id, name="dev"),
                MembershipRow(project_id=project_id, user_id=user_id, role=Role.ADMIN),
            ):
                await session.merge(row)
                await session.flush()


async def _lock_project(session: AsyncSession, project_id: UUID) -> None:
    locked = await session.scalar(
        select(ProjectRow.id).where(ProjectRow.id == project_id).with_for_update()
    )
    if locked is None:
        raise ProjectNotFound(str(project_id))


async def _locked_member(session: AsyncSession, project_id: UUID, user_id: UUID) -> MembershipRow:
    await _lock_project(session, project_id)
    member = await session.get(MembershipRow, (project_id, user_id))
    if member is None:
        raise MemberNotFound(str(user_id))
    return member


async def _keep_an_admin(session: AsyncSession, leaving: MembershipRow) -> None:
    """`leaving` перестаёт быть Admin: должен остаться другой."""
    if leaving.role != Role.ADMIN:
        return
    admins = await session.scalar(
        select(func.count())
        .select_from(MembershipRow)
        .where(MembershipRow.project_id == leaving.project_id, MembershipRow.role == Role.ADMIN)
    )
    if (admins or 0) <= 1:
        raise LastAdmin(str(leaving.project_id))
