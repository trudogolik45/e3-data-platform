"""REST API v1 Identity & Access.

Organization и Project создаёт команда платформы (`E3_PLATFORM_TOKEN`), Membership
ведёт Admin проекта.
"""

from typing import Annotated, Self
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field, model_validator

from e3.identity.access import Principal, ProjectAccessDenied, Role, authorize
from e3.identity.authentication import current_principal, require_platform_team
from e3.identity.service import (
    IdentityService,
    LastAdmin,
    MemberExists,
    MemberNotFound,
    OrganizationNotFound,
    UserAmbiguous,
    UserNotFound,
)

router = APIRouter(prefix="/api/v1", tags=["identity"])


class UserRef(BaseModel):
    """Существующий User: по `user_id` или по подтверждённому email — ровно одно."""

    user_id: UUID | None = None
    email: str | None = None

    @model_validator(mode="after")
    def exactly_one(self) -> Self:
        if (self.user_id is None) == (self.email is None):
            raise ValueError("нужен ровно один из user_id и email")
        return self


class ProjectRoleOut(BaseModel):
    project_id: UUID
    role: Role


class MeOut(BaseModel):
    user_id: UUID
    projects: list[ProjectRoleOut]


class CreateOrganizationIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class OrganizationOut(BaseModel):
    organization_id: UUID
    name: str


class CreateProjectIn(BaseModel):
    organization_id: UUID
    name: str = Field(min_length=1, max_length=200)
    admin: UserRef


class ProjectOut(BaseModel):
    project_id: UUID
    organization_id: UUID
    name: str


class AddMemberIn(UserRef):
    role: Role


class ChangeRoleIn(BaseModel):
    role: Role


class MemberOut(BaseModel):
    user_id: UUID
    email: str | None
    role: Role


def get_identity(request: Request) -> IdentityService:
    identity: IdentityService = request.app.state.identity
    return identity


IdentityDep = Annotated[IdentityService, Depends(get_identity)]
PrincipalDep = Annotated[Principal, Depends(current_principal)]
PlatformTeam = [Depends(require_platform_team)]


@router.get("/me")
async def me(principal: PrincipalDep) -> MeOut:
    return MeOut(
        user_id=principal.user_id,
        projects=[
            ProjectRoleOut(project_id=project_id, role=role)
            for project_id, role in principal.project_roles.items()
        ],
    )


@router.post("/organizations", status_code=status.HTTP_201_CREATED, dependencies=PlatformTeam)
async def create_organization(body: CreateOrganizationIn, identity: IdentityDep) -> OrganizationOut:
    organization_id = await identity.create_organization(body.name)
    return OrganizationOut(organization_id=organization_id, name=body.name)


@router.post("/projects", status_code=status.HTTP_201_CREATED, dependencies=PlatformTeam)
async def create_project(body: CreateProjectIn, identity: IdentityDep) -> ProjectOut:
    admin = await _find_user(identity, body.admin)
    try:
        project_id = await identity.create_project(body.organization_id, body.name, admin=admin)
    except OrganizationNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Organization не найден") from None
    return ProjectOut(project_id=project_id, organization_id=body.organization_id, name=body.name)


@router.get("/projects/{project_id}/members")
async def list_members(
    project_id: UUID, identity: IdentityDep, principal: PrincipalDep
) -> list[MemberOut]:
    _authorize(principal, project_id, Role.VIEWER)
    return [
        MemberOut(user_id=m.user_id, email=m.email, role=m.role)
        for m in await identity.members(project_id)
    ]


@router.post("/projects/{project_id}/members", status_code=status.HTTP_201_CREATED)
async def add_member(
    project_id: UUID, body: AddMemberIn, identity: IdentityDep, principal: PrincipalDep
) -> MemberOut:
    _authorize(principal, project_id, Role.ADMIN)
    user_id = await _find_user(identity, body)
    try:
        await identity.add_member(project_id, user_id, body.role)
    except MemberExists:
        raise HTTPException(status.HTTP_409_CONFLICT, "уже участник Project") from None
    return await _member_out(identity, project_id, user_id)


@router.put("/projects/{project_id}/members/{user_id}")
async def change_role(
    project_id: UUID,
    user_id: UUID,
    body: ChangeRoleIn,
    identity: IdentityDep,
    principal: PrincipalDep,
) -> MemberOut:
    _authorize(principal, project_id, Role.ADMIN)
    try:
        await identity.change_role(project_id, user_id, body.role)
    except MemberNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "не участник Project") from None
    except LastAdmin:
        raise _last_admin() from None
    return await _member_out(identity, project_id, user_id)


@router.delete("/projects/{project_id}/members/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_member(
    project_id: UUID, user_id: UUID, identity: IdentityDep, principal: PrincipalDep
) -> Response:
    _authorize(principal, project_id, Role.ADMIN)
    try:
        await identity.remove_member(project_id, user_id)
    except MemberNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "не участник Project") from None
    except LastAdmin:
        raise _last_admin() from None
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def _find_user(identity: IdentityService, ref: UserRef) -> UUID:
    try:
        return await identity.find_user(user_id=ref.user_id, email=ref.email)
    except UserNotFound:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "User не найден: он должен хотя бы раз войти"
        ) from None
    except UserAmbiguous:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "email есть у нескольких User, укажите user_id"
        ) from None


async def _member_out(identity: IdentityService, project_id: UUID, user_id: UUID) -> MemberOut:
    member = await identity.member(project_id, user_id)
    return MemberOut(user_id=member.user_id, email=member.email, role=member.role)


def _authorize(principal: Principal, project_id: UUID, minimum: Role) -> None:
    try:
        authorize(principal, project_id, minimum)
    except ProjectAccessDenied:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "нет доступа к Project") from None


def _last_admin() -> HTTPException:
    return HTTPException(status.HTTP_409_CONFLICT, "в Project должен остаться Admin")
