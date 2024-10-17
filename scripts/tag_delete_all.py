import subprocess

def remove_tags():
    # tagname이 순으로 내림차순 정렬
    tags = subprocess.run(["git", "tag"], check=True, capture_output=True, text=True).stdout.splitlines()
    if len(tags) == 0:
        return

    for _, tag_name in enumerate(tags):
        subprocess.run(["git", "tag", "-d", tag_name], check=True)
        subprocess.run(["git", "push", "origin", f":refs/tags/{tag_name}"], check=True)

if __name__ == "__main__":
    remove_tags()
