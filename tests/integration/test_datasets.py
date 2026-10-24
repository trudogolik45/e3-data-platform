from uuid import uuid4

import pytest

from tests.integration.conftest import Api

pytestmark = pytest.mark.integration


async def test_dataset_names_are_unique_after_normalization(api: Api) -> None:
    suffix = uuid4().hex

    first = await api.create_dataset(f"Sales  {suffix}")
    lower = await api.create_dataset(f"sales {suffix}")
    padded = await api.create_dataset(f"  sales   {suffix} ")

    assert first.status_code == 201
    assert first.json()["name"] == f"Sales  {suffix}"
    assert (lower.status_code, padded.status_code) == (409, 409)


async def test_dataset_in_foreign_project_is_forbidden(api: Api) -> None:
    response = await api.create_dataset("sales", project_id=uuid4())

    assert response.status_code == 403


async def test_blank_dataset_name_is_invalid(api: Api) -> None:
    response = await api.create_dataset("   ")

    assert response.status_code == 422


async def test_versions_of_unknown_dataset_are_not_found(api: Api) -> None:
    response = await api.client.get(f"/api/v1/datasets/{uuid4()}/versions")

    assert response.status_code == 404
