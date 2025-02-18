import os


def load_dot_env(file_path: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")

    with open(file_path, "r") as file:
        for line in file:
            # Remove whitespace and ignore comments
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Split key and value
            if "=" in line:
                key, value = line.split("=", 1)
                key, value = key.strip(), value.strip()

                # Remove quotes around the value if present
                if (value.startswith('"') and value.endswith('"')) or (
                    value.startswith("'") and value.endswith("'")
                ):
                    value = value[1:-1]

                # Set the environment variable
                os.environ[key] = value
