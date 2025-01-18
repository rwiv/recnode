import os


def load_env(file_path: str):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                # Split into key and value
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")  # Remove surrounding quotes if any

                    # Set the environment variable
                    os.environ[key] = value
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    except Exception as e:
        raise Exception(f"An error occurred while loading the .env file: {e}")
