
def user_choice(prompt, choices=("yes", "no"), default=None):
    """
    Prompts the user for confirmation.  The default value, if any, is capitalized.
    :param prompt: Information to display to the user.
    :param choices: an iterable of possible choices.
    :param default: default choice
    :return: the user's choice
    """
    choice_list = ', '.join((choice.title() if choice == default else choice for choice in choices))
    valid = False
    while not valid:
        response = input(prompt + ' [' + choice_list + ']: ')
        response = response if response else default
        valid = response in choices
    return response
