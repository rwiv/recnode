from pathlib import Path


def find_project_root():
    current_path = Path(__file__).resolve()
    for parent in current_path.parents:
        if (parent / ".git").exists():
            return parent
    return current_path
