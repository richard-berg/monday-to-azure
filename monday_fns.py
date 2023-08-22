from dataclasses import asdict, dataclass
import json
from typing import Type
from typing_extensions import Self
from inflection import parameterize

import httpx

API_URL = "https://api.monday.com/v2"
API_VERSION = "2023-04"
DEFAULT_BOARD_ID = 4609409564


@dataclass(frozen=True)
class MondayUser:
    name: str
    email: str
    voice_part: str
    chorus_emails: str
    social_emails: str
    section_leader: str

    @classmethod
    def from_board_api(cls: Type[Self], board_item: dict[str, object]) -> Self:
        values = {"name": board_item["name"]}
        for column_value in board_item["column_values"]:  # type: ignore
            field = parameterize(column_value["title"], "_")
            if field in cls.__dataclass_fields__:
                values[field] = column_value["text"]
        return cls(**values)  # type: ignore

    def to_dict(self: Self) -> dict[str, str]:
        return asdict(self)

    @staticmethod
    def from_json(s: str) -> "MondayUser":
        values = json.loads(s)
        return MondayUser(**values)

    @staticmethod
    def to_json(obj: "MondayUser") -> str:
        return json.dumps(obj.to_dict())


async def get_monday_roster(api_key: str, webhook_event: dict) -> list[MondayUser]:
    query = """
    query RosterDump($boardId: Int!) {
        boards(ids: [$boardId]) {
            items(limit:1000) {
            name
            column_values() {
                title
                text
            }
            }
        }
    }
    """
    headers = {"Authorization": api_key, "API-Version": API_VERSION}
    vars = {"boardId": webhook_event.get("boardId", DEFAULT_BOARD_ID)}
    outer_json = {"query": query, "variables": vars}
    async with httpx.AsyncClient() as client:
        response = await client.post(url=API_URL, json=outer_json, headers=headers)
        j = json.loads(response.text)
        roster = [MondayUser.from_board_api(item) for item in j["data"]["boards"][0]["items"]]
        return roster
