from pathlib import Path
import hashlib


def calculate_file_sha256(filePath:Path)->str:
    digest=hashlib.sha256()
    filePath=Path(filePath)
    with filePath.open("rb") as file:
        while chunk:=file.read(1024*1024):
            digest.update(chunk)
    return digest.hexdigest()
