import re


PROGRAMMER_ROLE = "программист, разработчик"
DEVELOPMENT_LEAD_ROLE = "руководитель группы разработки"


PYTHON_PATTERN = re.compile(
    r"\bpython(?:\s*\d+(?:\.\d+)*)?\b|\bphyton\b",
    re.IGNORECASE,
)

FRAMEWORK_PATTERN = re.compile(
    r"\b(?:django|fastapi|flask|tornado|pyramid|bottle|sanic|aiohttp)\b",
    re.IGNORECASE,
)

DEVELOPER_TITLE_PATTERN = re.compile(
    r"\b(?:developer|разработчик|программист|backend|back[\s-]?end|"
    r"full[\s-]?stack|software\s+engineer|etl)\b|разработк",
    re.IGNORECASE,
)

TEACHING_TITLE_PATTERN = re.compile(
    r"тьютор|преподавател|учител",
    re.IGNORECASE,
)

OTHER_SPECIALIZATION_PATTERN = re.compile(
    r"c\s*/\s*c\+\+|c\+\+|(?<!\w)c#(?!\w)|с\+\+|"
    r"\b(?:go|golang|java|php|ruby|rust|kotlin|swift|scala|perl|sql|"
    r"bitrix|1с|javascript|typescript|node(?:\.js)?|unity|unreal)\b|"
    r"\.net|front[\s-]?end|фронтенд|"
    r"реверс[\s-]?инженер|reverse[\s-]?engineer|"
    r"микроконтроллер\w*|embedded",
    re.IGNORECASE,
)


def is_python_developer(row: dict) -> bool:
    role = str(row.get("role") or "").strip().casefold()
    name = str(row.get("name") or "")
    skills = str(row.get("key_skills") or "")

    if role not in {PROGRAMMER_ROLE, DEVELOPMENT_LEAD_ROLE}:
        return False

    if TEACHING_TITLE_PATTERN.search(name):
        return False

    python_in_name = bool(PYTHON_PATTERN.search(name))
    framework_in_name = bool(FRAMEWORK_PATTERN.search(name))

    if role == DEVELOPMENT_LEAD_ROLE:
        return python_in_name or framework_in_name

    development_title = bool(DEVELOPER_TITLE_PATTERN.search(name))

    if python_in_name:
        return development_title

    if framework_in_name:
        return (
            development_title
            and not OTHER_SPECIALIZATION_PATTERN.search(name)
        )

    if OTHER_SPECIALIZATION_PATTERN.search(name):
        return False

    return development_title and bool(
        PYTHON_PATTERN.search(skills)
        or FRAMEWORK_PATTERN.search(skills)
    )