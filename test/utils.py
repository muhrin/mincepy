import random
import string


def random_str(length=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))
