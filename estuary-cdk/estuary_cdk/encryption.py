import base64
from cryptography.fernet import Fernet
import hashlib


def _generate_cipher(key: str) -> Fernet:
    # The Fernet key must be 32 bytes long & in a base64 URL-safe format.
    hashed_key = hashlib.sha256(key.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(hashed_key)

    return Fernet(fernet_key)


def encrypt(value: str, key: str) -> str:
    cipher = _generate_cipher(key)

    return cipher.encrypt(value.encode()).decode()


def decrypt(value: str, key: str) -> str:
    cipher = _generate_cipher(key)

    return cipher.decrypt(value).decode()
