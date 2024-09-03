import asyncio
from dataclasses import dataclass
import logging
from typing import Callable, Type
from typing_extensions import Self

from asyncio_pool import AioPool
from azure.identity.aio import DefaultAzureCredential
from inflection import camelize, pluralize
from msgraph import GraphServiceClient
from msgraph.generated.groups.groups_request_builder import GroupsRequestBuilder
from msgraph.generated.models.group import Group
from msgraph.generated.models.invitation import Invitation
from msgraph.generated.models.open_type_extension import OpenTypeExtension
from msgraph.generated.models.reference_create import ReferenceCreate
from msgraph.generated.models.user import User
from msgraph.generated.users.users_request_builder import UsersRequestBuilder

from monday_fns import MondayUser

MSGRAPH_SCOPES = ["https://graph.microsoft.com/.default"]

SERVICE_DOMAIN = "sync.cantorinewyork.com"
SERVICE_URL = f"https://{SERVICE_DOMAIN}"

MONDAY_TO_MSGRAPH_FIELD_MAPPING = {
    "email": "mail",
    "name": "display_name",
    "voice_part": "department",
}

USER_FIELDS = {
    "id",
    "mail",
    "display_name",
    "mobile_phone",
    "department",
    "job_title",
}
USER_KEYS = {
    "id",
    "mail",
}  # we consider these 1-to-1, i.e. treat email address as the natural key
USER_FIELDS_CAN_PATCH = USER_FIELDS - USER_KEYS

EXTENSION_TYPE = ".".join(reversed(SERVICE_DOMAIN.split(".")))
EXTENSION_FIELDS = {"chorus_emails", "social_emails", "section_leader"}

MAX_CONCURRENCY = 10
MAX_USERS = 999

YES = "Yes"


@dataclass
class EmailList:
    email_prefix: str
    roster_filter: Callable[[MondayUser], bool]

    @classmethod
    def from_section(cls: Type[Self], section: str) -> "EmailList":
        def satb_filter(u: MondayUser) -> bool:
            voice_part_satb = u.voice_part.lower().split(" ")[0]
            voice_part_plural = pluralize(voice_part_satb)
            return u.chorus_emails == YES and section == voice_part_plural

        return EmailList(section, satb_filter)


EMAIL_LISTS = [
    EmailList("active", lambda u: u.chorus_emails == YES),
    EmailList("social", lambda u: u.social_emails == YES),
    EmailList("sectionleaders", lambda u: u.section_leader == YES),
    EmailList.from_section("sopranos"),
    EmailList.from_section("altos"),
    EmailList.from_section("tenors"),
    EmailList.from_section("basses"),
]

USERS_IN_EVERY_EMAIL_LIST = {
    "richard.berg@cantorinewyork.com",
    "markshapiro212@gmail.com",
    "chelsea.harvey@cantorinewyork.com",
    "krdinicola@gmail.com",
    "cantoriscores@gmail.com",
}


async def _create_client() -> GraphServiceClient:
    async with DefaultAzureCredential() as credential:
        return GraphServiceClient(credentials=credential, scopes=MSGRAPH_SCOPES)


async def _get_aad_users(client: GraphServiceClient, fields) -> list[User]:
    request_configuration = UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
        query_parameters=UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
            select=[camelize(f, False) for f in fields],
            expand=["extensions"],
            top=MAX_USERS,
        )
    )
    aad_users = await client.users.get(request_configuration=request_configuration)

    if aad_users and aad_users.value:
        logging.info(f"Found {len(aad_users.value)} users in AAD")
        return aad_users.value
    else:
        raise RuntimeError("Failed to query users from AAD")


async def upsert_aad_users(roster: list[MondayUser]) -> dict[str, str]:
    """Given a desired roster, create and/or update AAD users as needed.

    Returns a map from email address -> AAD directory ID
    """
    client = await _create_client()

    roster_users = [_monday_user_to_msgraph_user(u) for u in roster]
    aad_users = await _get_aad_users(client, USER_FIELDS)
    email_to_aad_user = {u.mail: u for u in aad_users}
    email_to_aad_directory_id = {u.mail: u.id for u in aad_users}

    async def upsert_aad_user(roster_user: User) -> None:
        if not roster_user.mail:
            logging.warn(f"'{roster_user.display_name}' has no email address -- skipping!")
            return
        try:
            aad_user = email_to_aad_user[roster_user.mail]
        except KeyError:
            new_user_id = await _create_AAD_guest(client, roster_user.mail, roster_user.display_name)
            aad_user = User(id=new_user_id, mail=roster_user.mail)
            email_to_aad_directory_id[aad_user.mail] = aad_user.id

        await _update_AAD_user_if_necessary(client, aad_user, roster_user)

    await _run_concurrently(upsert_aad_user, roster_users)
    return email_to_aad_directory_id


async def _run_concurrently(fn, iterable):
    async with AioPool(size=MAX_CONCURRENCY) as pool:
        results = await pool.map(fn, iterable)
        for r in results:
            if isinstance(r, Exception):
                raise r
        return results


def _monday_user_to_msgraph_user(monday_user: MondayUser) -> User:
    extension = OpenTypeExtension(
        id=EXTENSION_TYPE,
        extension_name=EXTENSION_TYPE,
        additional_data={f: getattr(monday_user, f) for f in EXTENSION_FIELDS},
    )
    msgraph_user = User(extensions=[extension])
    for monday_field, msgraph_field in MONDAY_TO_MSGRAPH_FIELD_MAPPING.items():
        setattr(msgraph_user, msgraph_field, getattr(monday_user, monday_field))
    return msgraph_user


async def _create_AAD_guest(client: GraphServiceClient, mail: str, display_name: str) -> str:
    """Create a guest user in AAD with the given email & display name. Returns the AAD directory ID of the new User."""
    logging.info(f"Will add {mail} as AAD guest")
    request_body = Invitation()
    request_body.invited_user_email_address = mail
    request_body.invited_user_display_name = display_name
    request_body.invite_redirect_url = SERVICE_URL
    request_body.send_invitation_message = False
    result = await client.invitations.post(body=request_body)
    logging.info(f"Added {mail} as AAD guest with ID {result.invited_user.id}")

    return result.invited_user.id


async def _update_AAD_user_if_necessary(
    client: GraphServiceClient, aad_user: User, roster_user: User
) -> None:
    await _update_AAD_user_properties(client, aad_user, roster_user)
    await _update_AAD_user_extensions(client, aad_user, roster_user)


async def _update_AAD_user_properties(client: GraphServiceClient, aad_user: User, roster_user: User) -> None:
    request_body = User()
    need_update = False
    for field in USER_FIELDS_CAN_PATCH:
        requested_field = getattr(roster_user, field)
        aad_field = getattr(aad_user, field)
        # Don't clear fields (e.g. job_title) that happen to be missing from the roster
        if requested_field and requested_field != aad_field:
            setattr(request_body, field, requested_field)
            need_update = True

    if need_update:
        logging.info(f"Will update properties on {aad_user.mail}")
        await client.users.by_user_id(aad_user.id).patch(body=request_body)
        logging.info(f"Updated properties on {aad_user.mail}")
    else:
        logging.info(f"Properties of {aad_user.mail} are already in sync")


async def _update_AAD_user_extensions(client: GraphServiceClient, aad_user: User, roster_user: User) -> None:
    aad_extensions_by_id = {e.id: e for e in (aad_user.extensions or [])}
    for extension in roster_user.extensions or []:
        try:
            aad_extension = aad_extensions_by_id[extension.id]
            if extension.additional_data == aad_extension.additional_data:
                logging.info(f"Extended properties of {aad_user.mail}/{extension.id} are already in sync")
            else:
                logging.info(f"Will update extended properties on {aad_user.mail}/{extension.id}")
                await client.users.by_user_id(aad_user.id).extensions.by_extension_id(extension.id).patch(
                    extension
                )

                logging.info(f"Updated extended properties on {aad_user.mail}/{extension.id}")
        except KeyError:
            logging.info(f"Will add extended properties to {aad_user.mail}/{extension.id}")
            await client.users.by_user_id(aad_user.id).extensions.post(extension)
            logging.info(f"Added extended properties to {aad_user.mail}/{extension.id}")


async def sync_all_group_membership(roster: list[MondayUser], email_to_user_id: dict[str, str] = {}) -> None:
    client = await _create_client()

    if not email_to_user_id:
        aad_users = await _get_aad_users(client, USER_KEYS)
        email_to_user_id = {u.mail: u.id for u in aad_users}

    aad_groups = await _get_aad_groups(client)
    email_prefix_to_aad_group = {g.mail.split("@")[0]: g for g in aad_groups}

    async def add_group_member(group: Group, email: str) -> None:
        try:
            user_id = email_to_user_id[email]
        except KeyError:
            logging.warn(f"Cannot add '{email}' to {group.display_name} because it's not in AAD!")
            return

        logging.info(f"Will add {email} to {group.display_name}")
        request_body = ReferenceCreate()
        request_body.odata_id = f"https://graph.microsoft.com/v1.0/directoryObjects/{user_id}"
        await client.groups.by_group_id(group.id).members.ref.post(body=request_body)
        logging.info(f"Added {email} to {group.display_name}")

    async def remove_group_member(group: Group, email: str) -> None:
        logging.info(f"Will remove {email} from {group.display_name}")
        user_id = email_to_user_id[email]
        await client.groups.by_group_id(group.id).members.by_directory_object_id(user_id).ref.delete()
        logging.info(f"Removed {email} from {group.display_name}")

    async def sync_one_group_membership(email_list: EmailList, pool: AioPool) -> None:
        aad_group = email_prefix_to_aad_group[email_list.email_prefix]
        current_members = await _get_aad_group_members(client, aad_group)

        current_emails = {m.mail for m in current_members}
        roster_emails = {u.email for u in roster if email_list.roster_filter(u)}
        target_emails = roster_emails | USERS_IN_EVERY_EMAIL_LIST
        emails_to_add = target_emails - current_emails
        emails_to_remove = current_emails - target_emails

        for email in emails_to_add:
            pool.spawn_n(add_group_member(aad_group, email))

        for email in emails_to_remove:
            pool.spawn_n(remove_group_member(aad_group, email))

    async with AioPool(size=MAX_CONCURRENCY) as pool:
        for email_list in EMAIL_LISTS:
            pool.spawn_n(sync_one_group_membership(email_list, pool))


async def _get_aad_groups(client: GraphServiceClient) -> list[Group]:
    request_configuration = GroupsRequestBuilder.GroupsRequestBuilderGetRequestConfiguration(
        query_parameters=GroupsRequestBuilder.GroupsRequestBuilderGetQueryParameters(
            select=["id", "displayName", "mail"],
        ),
    )
    aad_groups = await client.groups.get(request_configuration=request_configuration)
    if aad_groups and aad_groups.value:
        logging.info(f"Found {len(aad_groups.value)} groups in AAD")
        return aad_groups.value
    else:
        raise RuntimeError("Failed to query groups from AAD")


async def _get_aad_group_members(client: GraphServiceClient, group: Group) -> list[User]:
    members = await client.groups.by_group_id(group.id).members.get()
    if members and hasattr(members, "value"):
        logging.info(f"Found {len(members.value)} members of {group.mail}")
        return members.value
    else:
        raise RuntimeError(f"Failed to query {group.mail} membership from AAD")


async def get_group_member_names(email_prefix: str) -> list[str]:
    client = await _create_client()
    request_configuration = GroupsRequestBuilder.GroupsRequestBuilderGetRequestConfiguration(
        query_parameters=GroupsRequestBuilder.GroupsRequestBuilderGetQueryParameters(
            select=["id", "mail"], filter=f"startsWith(mail, '{email_prefix}@')"
        ),
    )
    groups = await client.groups.get(request_configuration=request_configuration)
    if len(groups.value) != 1:
        raise ValueError(f"Could not find unique group with email prefix {email_prefix}@")
    group = groups.value[0]
    members = await _get_aad_group_members(client, group)
    return [u.display_name for u in members]


async def get_group_email_prefixes() -> list[str]:
    client = await _create_client()
    groups = await _get_aad_groups(client)
    return [g.mail.split("@")[0] for g in groups]


async def main():
    logging.basicConfig(level=logging.INFO)
    roster = [
        MondayUser(
            email="thomasinb@gmail.com",
            name="Thomasin Bentley",
            voice_part="Alto 1",
            chorus_emails="",
            social_emails="",
            section_leader="",
        ),
        MondayUser(
            email="tristan.lesso@cantorinewyork.com",
            name="Tristan Lesso",
            voice_part="",
            chorus_emails="",
            social_emails="",
            section_leader="",
        ),
        MondayUser(
            email="test@cantorinewyork.com",
            name="Test User Internal",
            voice_part="",
            chorus_emails="",
            social_emails=YES,
            section_leader="",
        ),
    ]
    email_to_user_id = await upsert_aad_users(roster)
    await sync_all_group_membership(roster, email_to_user_id)


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
