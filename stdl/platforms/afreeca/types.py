from dataclasses import dataclass


@dataclass
class AfreecaCredential:
    username: str
    password: str

    def to_options(self) -> dict:
        return {
            "username": self.username,
            "password": self.password,
        }
