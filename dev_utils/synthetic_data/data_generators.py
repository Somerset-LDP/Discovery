import random


def generate_valid_nhs_number() -> str:
    """Generate a valid NHS number with correct check digit."""
    digits = [random.randint(0, 9) for _ in range(9)]
    total = sum(d * (10 - i) for i, d in enumerate(digits))
    remainder = total % 11
    check_digit = 11 - remainder
    if check_digit == 11:
        check_digit = 0
    if check_digit == 10:
        return generate_valid_nhs_number()
    digits.append(check_digit)
    return ''.join(str(d) for d in digits)


def generate_invalid_nhs_number() -> str:
    """Generate an invalid NHS number with incorrect check digit."""
    valid = generate_valid_nhs_number()
    invalid_digit = (int(valid[-1]) + random.randint(1, 9)) % 10
    return valid[:-1] + str(invalid_digit)


def generate_random_name() -> str:
    """Generate a random first name using syllable combinations."""
    syllables = ["an", "jo", "li", "mi", "el", "ra", "ka", "ta", "vi", "sa", "da", "le", "no", "ri", "mo", "ke", "zu",
                 "fa", "te", "po"]
    name = ''.join(random.choices(syllables, k=random.randint(2, 3))).capitalize()
    return name


def generate_random_surname() -> str:
    """Generate a random surname using syllable combinations."""
    syllables = ["son", "man", "ley", "ton", "ford", "wood", "field", "well", "stone", "brook", "hall", "smith", "king",
                 "ward", "dale", "burn", "wall", "shaw", "banks", "wright"]
    surname = ''.join(random.choices(syllables, k=random.randint(2, 3))).capitalize()
    return surname


def generate_random_dob() -> str:
    """Generate a random date of birth in YYYY-MM-DD format."""
    year = random.randint(1940, 2020)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year:04d}-{month:02d}-{day:02d}"


def generate_random_postcode() -> str:
    """Generate a random UK-style postcode."""
    return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2)) + str(random.randint(10, 99)) + ' ' + str(
        random.randint(1, 9)) + ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
