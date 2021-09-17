from cryptography.fernet import Fernet

def write_key():
    """
    Generates a key and save it into a file
    """
    key = Fernet.generate_key()
    with open("/opt/app/keys/key.key", "wb") as key_file:
        key_file.write(key)


write_key()

