from logging import Logger
from typing import Dict, Set

from estuary_cdk.http import HTTPSession
from pydantic import BaseModel

from .models import FacebookResource, AdAccount


class BusinessUser(BaseModel):
    class Business(BaseModel):
        id: str
        name: str

    id: str
    business: Business


class BusinessUsersResponse(BaseModel):
    data: list[BusinessUser] = []


class AssignedUser(BaseModel):
    id: str
    tasks: list[str] = []


class AssignedUsersResponse(BaseModel):
    data: list[AssignedUser] = []


class ResourcePermissionRequirement(BaseModel):
    field_name: str
    required_permissions: Set[str]
    description: str | None = None


class PermissionManager:
    def __init__(self, http_session: HTTPSession, base_url: str, log: Logger):
        self.http = http_session
        self.base_url = base_url
        self.log = log

        self._permissions_cache: Dict[str, Set[str]] = {}
        self._filtered_fields_cache: Dict[str, Dict[str, list[str]]] = {}

    async def get_account_permissions(self, account_id: str) -> Set[str]:
        if account_id in self._permissions_cache:
            return self._permissions_cache[account_id]

        permissions = set()
        try:
            business_user_response = BusinessUsersResponse.model_validate_json(
                await self.http.request(
                    self.log,
                    f"{self.base_url}/me/business_users",
                    params={"fields": "business,id"},
                )
            )

            for business_user in business_user_response.data:
                business_id = business_user.business.id
                if not business_id:
                    continue

                assigned_users_response = AssignedUsersResponse.model_validate_json(
                    await self.http.request(
                        self.log,
                        f"{self.base_url}/act_{account_id}/assigned_users",
                        params={"business": business_id, "fields": "id,tasks"},
                    )
                )

                for assigned_user in assigned_users_response.data:
                    if business_user.id == assigned_user.id:
                        permissions.update(assigned_user.tasks)

        except Exception as e:
            self.log.warning(f"Failed to get permissions for account {account_id}: {e}")
            # Return empty set if we can't determine permissions
            permissions = set()

        self._permissions_cache[account_id] = permissions
        return permissions

    def get_resource_permission_requirements(
        self, resource_model: type[FacebookResource]
    ) -> list[ResourcePermissionRequirement]:
        match resource_model:
            case type() if resource_model is AdAccount:
                return [
                    ResourcePermissionRequirement(
                        field_name="funding_source_details",
                        required_permissions={"MANAGE"},
                        description="Access to funding source details requires MANAGE permission",
                    ),
                    ResourcePermissionRequirement(
                        field_name="is_prepay_account",
                        required_permissions={"MANAGE"},
                        description="Access to prepay account status requires MANAGE permission",
                    ),
                    ResourcePermissionRequirement(
                        field_name="owner",
                        required_permissions={"business_management"},
                        description="Access to owner field requires business_management permission",
                    ),
                ]
            case _:
                return []

    async def get_filtered_fields(
        self, resource_model: type[FacebookResource], account_id: str
    ) -> list[str]:
        resource_name = resource_model.__name__

        if (
            resource_name in self._filtered_fields_cache
            and account_id in self._filtered_fields_cache[resource_name]
        ):
            return self._filtered_fields_cache[resource_name][account_id]

        available_fields = resource_model.fields.copy()
        permission_requirements = self.get_resource_permission_requirements(
            resource_model
        )

        if permission_requirements:
            user_permissions = await self.get_account_permissions(account_id)

            filtered_fields = []
            removed_fields = []

            for field in available_fields:
                field_requirements = next(
                    (req for req in permission_requirements if req.field_name == field),
                    None,
                )

                match field_requirements:
                    case ResourcePermissionRequirement():
                        has_required_permissions = any(
                            perm in user_permissions
                            for perm in field_requirements.required_permissions
                        )
                        if has_required_permissions:
                            filtered_fields.append(field)
                        else:
                            removed_fields.append(field)
                            self.log.debug(
                                f"Removed field '{field}' - requires permissions: {field_requirements.required_permissions}"
                            )
                    case None:
                        filtered_fields.append(field)

            available_fields = filtered_fields

            if removed_fields:
                self.log.info(
                    f"Filtered {len(removed_fields)} fields from {resource_name} for account {account_id}: {removed_fields}"
                )

        if resource_name not in self._filtered_fields_cache:
            self._filtered_fields_cache[resource_name] = {}
        self._filtered_fields_cache[resource_name][account_id] = available_fields

        return available_fields
