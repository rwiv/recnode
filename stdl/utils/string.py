import random
import string


def random_string(length: int = 8) -> str:
    letters = string.ascii_letters + string.digits
    return "".join(random.choices(letters, k=length))
