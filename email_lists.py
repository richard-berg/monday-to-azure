from dataclasses import asdict, dataclass
from typing import Callable, Collection, Type

from inflection import pluralize
from typing_extensions import Self

from monday_fns import MondayUser

YES = "Yes"


@dataclass(frozen=True)
class EmailListMember:
    email: str
    name: str

    @staticmethod
    def from_monday_user(m: MondayUser):
        return EmailListMember(email=m.email, name=m.name)

    def as_dict(self):
        return asdict(self)


@dataclass(frozen=True)
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

    def get_members(self, roster: list[MondayUser]) -> list[EmailListMember]:
        members = [
            EmailListMember.from_monday_user(u)
            for u in roster
            if self.roster_filter(u) or u.email in USERS_IN_EVERY_EMAIL_LIST
        ]

        # handle USERS_IN_EVERY_EMAIL_LIST who aren't on the roster
        missing_emails = USERS_IN_EVERY_EMAIL_LIST - {m.email for m in members}
        for missing_email in missing_emails:
            members.append(EmailListMember(email=missing_email, name=""))

        return members


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


def pivot_by_email(members: Collection[EmailListMember]) -> dict[str, EmailListMember]:
    return {m.email: m for m in members}
