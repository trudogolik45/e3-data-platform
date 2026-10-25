import asyncio
from uuid import UUID, uuid4

import pytest

from e3.api.app import create_app
from e3.identity import (
    Identity,
    IdentityService,
    LastAdmin,
    MemberExists,
    MemberNotFound,
    Role,
    UserAmbiguous,
    UserNotFound,
)
from e3.settings import Settings

pytestmark = pytest.mark.integration

ISSUER = "https://idp.test/e3"


@pytest.fixture
def identity(settings: Settings) -> IdentityService:
    return create_app(settings).state.identity


def person(subject: str | None = None, *, email: str | None = None, verified: bool = True):
    subject = subject or uuid4().hex
    return Identity(ISSUER, subject, email or f"{subject}@e3.test", verified)


async def signed_in(identity: IdentityService, **kwargs: object) -> UUID:
    return await identity.sign_in(person(**kwargs))  # type: ignore[arg-type]


async def new_project(identity: IdentityService, admin: UUID) -> UUID:
    organization = await identity.create_organization(f"org-{uuid4()}")
    project = await identity.create_project(organization, f"project-{uuid4()}", admin=admin)
    return project


async def test_first_sign_in_creates_user_and_next_one_reuses_it(
    identity: IdentityService,
) -> None:
    subject = uuid4().hex
    old, new = f"old-{subject}@e3.test", f"new-{subject}@e3.test"
    first = await identity.sign_in(person(subject, email=old))

    again = await identity.sign_in(person(subject, email=new))

    assert again == first
    assert await identity.find_user(email=new) == first
    with pytest.raises(UserNotFound):
        await identity.find_user(email=old)


async def test_same_subject_from_another_issuer_is_another_user(
    identity: IdentityService,
) -> None:
    subject = uuid4().hex
    ours = await identity.sign_in(person(subject))

    theirs = await identity.sign_in(Identity("https://other.test", subject, None, False))

    assert theirs != ours


async def test_project_creator_admin_gets_admin_role(identity: IdentityService) -> None:
    admin = await signed_in(identity)

    project = await new_project(identity, admin)

    assert await identity.role_of(admin, project) == Role.ADMIN
    principal = await identity.principal(admin)
    assert principal.project_roles == {project: Role.ADMIN}


async def test_project_needs_existing_admin(identity: IdentityService) -> None:
    organization = await identity.create_organization(f"org-{uuid4()}")

    with pytest.raises(UserNotFound):
        await identity.create_project(organization, "p", admin=uuid4())


async def test_admin_adds_changes_and_removes_member(identity: IdentityService) -> None:
    admin, member = await signed_in(identity), await signed_in(identity)
    project = await new_project(identity, admin)

    await identity.add_member(project, member, Role.VIEWER)
    await identity.change_role(project, member, Role.EDITOR)
    assert await identity.role_of(member, project) == Role.EDITOR
    assert {m.user_id: m.role for m in await identity.members(project)} == {
        admin: Role.ADMIN,
        member: Role.EDITOR,
    }

    await identity.remove_member(project, member)

    assert await identity.role_of(member, project) is None


async def test_member_cannot_be_added_twice(identity: IdentityService) -> None:
    admin, member = await signed_in(identity), await signed_in(identity)
    project = await new_project(identity, admin)
    await identity.add_member(project, member, Role.VIEWER)

    with pytest.raises(MemberExists):
        await identity.add_member(project, member, Role.EDITOR)


async def test_unknown_member_cannot_be_changed_or_removed(identity: IdentityService) -> None:
    project = await new_project(identity, await signed_in(identity))

    with pytest.raises(MemberNotFound):
        await identity.change_role(project, uuid4(), Role.VIEWER)
    with pytest.raises(MemberNotFound):
        await identity.remove_member(project, uuid4())


async def test_last_admin_cannot_be_demoted_or_removed(identity: IdentityService) -> None:
    admin = await signed_in(identity)
    project = await new_project(identity, admin)

    with pytest.raises(LastAdmin):
        await identity.change_role(project, admin, Role.EDITOR)
    with pytest.raises(LastAdmin):
        await identity.remove_member(project, admin)
    assert await identity.role_of(admin, project) == Role.ADMIN


async def test_parallel_demotions_keep_one_admin(identity: IdentityService) -> None:
    first, second = await signed_in(identity), await signed_in(identity)
    project = await new_project(identity, first)
    await identity.add_member(project, second, Role.ADMIN)

    results = await asyncio.gather(
        identity.change_role(project, first, Role.VIEWER),
        identity.change_role(project, second, Role.VIEWER),
        return_exceptions=True,
    )

    assert sum(isinstance(result, LastAdmin) for result in results) == 1
    roles = {m.role for m in await identity.members(project)}
    assert Role.ADMIN in roles


async def test_unverified_email_is_not_searchable(identity: IdentityService) -> None:
    email = f"{uuid4().hex}@e3.test"
    await signed_in(identity, email=email, verified=False)

    with pytest.raises(UserNotFound):
        await identity.find_user(email=email)


async def test_email_shared_by_two_users_is_ambiguous(identity: IdentityService) -> None:
    email = f"{uuid4().hex}@e3.test"
    await signed_in(identity, email=email)
    await signed_in(identity, email=email)

    with pytest.raises(UserAmbiguous):
        await identity.find_user(email=email)


async def test_user_is_found_by_id(identity: IdentityService) -> None:
    user = await signed_in(identity)

    assert await identity.find_user(user_id=user) == user
    with pytest.raises(UserNotFound):
        await identity.find_user(user_id=uuid4())
