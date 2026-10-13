"""Keep customer identity separate from names shown in orders and messages."""

import pandas as pd

from .facebook import normalize_fb_target


def _text(value: object) -> str:
    return "" if pd.isna(value) else str(value).strip()


def with_customer_labels(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if {"ClienteKey", "ClienteNome"}.issubset(out.columns):
        return out
    names = out["Cliente"].map(_text)
    users = out.get("UserId", out.get("User ID", pd.Series("", index=out.index))).map(_text).map(normalize_fb_target)
    profiles = out.get("ProfileId", out.get("Profile ID", pd.Series("", index=out.index))).map(_text).map(normalize_fb_target).str.casefold()
    profile_users: dict[str, set[str]] = {}
    for user, profile in zip(users, profiles):
        if user and profile:
            profile_users.setdefault(profile, set()).add(user)

    keys = []
    for name, user, profile in zip(names, users, profiles):
        linked_users = profile_users.get(profile, set())
        if not user and len(linked_users) == 1:
            user = next(iter(linked_users))
        keys.append(f"user:{user}" if user else f"profile:{profile}" if profile else f"name:{name}")
    known_by_name: dict[str, set[str]] = {}
    for name, key in zip(names, keys):
        if not key.startswith("name:"):
            known_by_name.setdefault(name, set()).add(key)
    for index, (name, key) in enumerate(zip(names, keys)):
        known = known_by_name.get(name, set())
        if key.startswith("name:") and len(known) == 1:
            keys[index] = next(iter(known))

    out["ClienteKey"] = keys
    out["ClienteNome"] = names
    name_by_key = out.groupby("ClienteKey")["ClienteNome"].agg(lambda group: sorted(set(group))[0]).to_dict()
    keys_by_name: dict[str, set[str]] = {}
    for key, name in name_by_key.items():
        keys_by_name.setdefault(name, set()).add(key)
    labels = {}
    for key, name in name_by_key.items():
        if len(keys_by_name[name]) == 1:
            labels[key] = name
        elif key.startswith("user:"):
            labels[key] = f"{name} · ID {key[5:]}"
        elif key.startswith("profile:"):
            labels[key] = f"{name} · @{key[8:]}"
        else:
            labels[key] = f"{name} · sem ID"
    # Imported names can contain any suffix, so resolve collisions until unique.
    used = set()
    for key in sorted(labels):
        original = labels[key]
        candidate = original
        counter = 1
        while candidate in used:
            candidate = f"{original} [{key}{f'/{counter}' if counter > 1 else ''}]"
            counter += 1
        labels[key] = candidate
        used.add(candidate)
    out["Cliente"] = out["ClienteKey"].map(labels)
    return out


def directory_keys(frame: pd.DataFrame) -> dict[str, str]:
    customers = with_customer_labels(frame)
    return dict(zip(customers["Cliente"], customers["ClienteKey"]))


def legacy_names(frame: pd.DataFrame) -> dict[str, str]:
    """Only allow old name-based directory entries when that name is unambiguous."""
    customers = with_customer_labels(frame)
    counts = customers.groupby("ClienteNome")["ClienteKey"].nunique()
    return {
        row["Cliente"]: row["ClienteNome"] for _, row in customers.drop_duplicates("ClienteKey").iterrows()
        if counts[row["ClienteNome"]] == 1
    }
