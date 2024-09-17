import asyncio
from dataclasses import dataclass
import logging
import os
import httpx
from email_lists import (
    EMAIL_LISTS,
    YES,
    EmailList,
    EmailListMember,
    pivot_by_email,
)
from monday_fns import MondayUser
from asyncio_pool import AioPool

MAX_CONCURRENCY = 10


@dataclass(frozen=True)
class GroupMember(EmailListMember):
    # deliveryMode: str
    # membershipId: str
    # membershipPageUrl: str
    pass


def _list_email(email_list: EmailList) -> str:
    return f"{email_list.email_prefix}@gaggle.cantorinewyork.com"


def _list_url(email_list: EmailList) -> str:
    return f"https://api.gaggle.email/api/v3/group/{_list_email(email_list)}"


async def _get_group_members(client: httpx.AsyncClient, email_list: EmailList) -> list[GroupMember]:
    url = f"{_list_url(email_list)}/members"
    response = await client.get(url=url)
    response.raise_for_status()
    members = [GroupMember(email=d["email"], name=d["name"]) for d in response.json()]
    logging.info(f"Found {len(members)} members of {_list_email(email_list)}")
    return members


async def _add_group_members(
    client: httpx.AsyncClient, email_list: EmailList, members_to_add: list[EmailListMember]
):
    if not members_to_add:
        return
    logging.info(f"Will add {members_to_add} to {_list_email(email_list)}")
    url = f"{_list_url(email_list)}/members"
    response = await client.put(url=url, json=[m.as_dict() for m in members_to_add])
    response.raise_for_status()


async def _update_group_member(client: httpx.AsyncClient, email_list: EmailList, member: EmailListMember):
    logging.info(f"Will update {member} in {_list_email(email_list)}")
    url = f"{_list_url(email_list)}/member/{member.email}"
    response = await client.post(url=url, json=member.as_dict())
    response.raise_for_status()


async def _remove_group_member(client: httpx.AsyncClient, email_list: EmailList, member: EmailListMember):
    logging.info(f"Will remove {member.email} from {_list_email(email_list)}")
    url = f"{_list_url(email_list)}/member/{member.email}"
    response = await client.delete(url=url)
    response.raise_for_status()


async def sync_all_group_membership(api_key: str, roster: list[MondayUser]) -> None:
    headers = {"x-api-key": api_key}
    async with httpx.AsyncClient(headers=headers, timeout=20) as client:

        async def sync_one_group(email_list: EmailList, pool: AioPool) -> None:
            current_members = pivot_by_email(await _get_group_members(client, email_list))
            target_members = pivot_by_email(email_list.get_members(roster))

            members_to_add = [
                member for email, member in target_members.items() if email not in current_members
            ]
            members_to_update = [
                member
                for email, member in current_members.items()
                if email in target_members and member.name != target_members[email].name
            ]
            members_to_remove = [
                member for email, member in current_members.items() if email not in target_members
            ]

            pool.spawn_n(_add_group_members(client, email_list, members_to_add))

            for member in members_to_update:
                pool.spawn_n(_update_group_member(client, email_list, member))

            for member in members_to_remove:
                pool.spawn_n(_remove_group_member(client, email_list, member))

        async with AioPool(size=MAX_CONCURRENCY) as pool:
            for email_list in EMAIL_LISTS:
                pool.spawn_n(sync_one_group(email_list, pool))


async def main():
    logging.basicConfig(level=logging.INFO)
    roster = [
        MondayUser(
            email="music@richardberg.net",
            name="Richard Berg",
            voice_part="Bass 2",
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
    api_key = os.environ["GAGGLE_API_KEY"]
    await sync_all_group_membership(api_key, roster)


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
