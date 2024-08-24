def convert_time(secs: int) -> str:
    hours = secs // 3600
    remaining_seconds = secs % 3600
    minutes = remaining_seconds // 60
    seconds = remaining_seconds % 60

    return f"{hours}:{minutes}:{seconds}"
