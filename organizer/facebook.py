import re


FB_PAGE_ID = "106851297526135"


def normalize_fb_target(v: str) -> str:
    s = str(v or "").strip()
    # Some CSV/Excel imports turn large numeric IDs into floats like "100023...332.0"
    s = re.sub(r"^(\d{5,})\.0$", r"\1", s)
    if s.lower() == "nan":
        return ""
    return s


def build_facebook_chat_url(user_id: str = "", profile_id: str = "") -> str:
    user_id = normalize_fb_target(user_id)
    profile_id = normalize_fb_target(profile_id)

    def is_numeric_id(v: str) -> bool:
        return bool(re.fullmatch(r"\d{5,}", v or ""))

    target = ""
    if is_numeric_id(user_id):
        target = user_id
    elif is_numeric_id(profile_id):
        target = profile_id
    else:
        target = user_id or profile_id
    if not target:
        return ""

    return (
        f"https://business.facebook.com/latest/inbox/all/?asset_id={FB_PAGE_ID}&mailbox_id={FB_PAGE_ID}"
        f"&selected_item_id={target}&thread_type=FB_MESSAGE"
    )


def build_facebook_profile_url(user_id: str = "", profile_id: str = "") -> str:
    user_id = normalize_fb_target(user_id)
    profile_id = normalize_fb_target(profile_id)

    if re.fullmatch(r"\d{5,}", user_id or ""):
        return f"https://www.facebook.com/profile.php?id={user_id}"

    if profile_id:
        return f"https://www.facebook.com/{profile_id}"

    return ""

