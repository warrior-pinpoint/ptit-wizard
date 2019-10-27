import random


GREETING_SAMPLE = {
    'greeting': [
        "What's up?",
        'How are u?'
    ],
    'morning': [
        'Good morning',
        'Morning'
    ],
    'evening': [
        'Good night',
        'Normal night pls',
        'Have a nightmare'
    ]
}


def generate_greeting(greeting_type):
    if greeting_type not in GREETING_SAMPLE.keys():
        raise ValueError('Greeting type is invalid')

    return GREETING_SAMPLE[greeting_type][random.randint(0, len(GREETING_SAMPLE[greeting_type]) - 1)]


def lucky_number(min_value, max_value):
    return random.randint(min_value, max_value)
