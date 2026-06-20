from typing import Any


def get_widget(dbutils_ref: Any, name: str, default: str = "") -> str:
    try:
        dbutils_ref.widgets.text(name, default)
        value = dbutils_ref.widgets.get(name)
        return value if value not in (None, "") else default
    except Exception:
        return default


def set_task_value(dbutils_ref: Any, key: str, value: str) -> None:
    try:
        dbutils_ref.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass
