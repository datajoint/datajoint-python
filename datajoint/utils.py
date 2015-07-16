import numpy as np


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


def group_by(rel, *attributes, sortby=None):
    r = rel.project(*attributes).fetch()
    dtype2 = np.dtype({name:r.dtype.fields[name] for name in attributes})
    r2 = np.unique(np.ndarray(r.shape, dtype2, r, 0, r.strides))
    r2.sort(order=sortby if sortby is not None else attributes)
    for nk in r2:
        restr = ' and '.join(["%s='%s'" % (fn, str(v)) for fn, v in zip(r2.dtype.names, nk)])
        if len(nk) == 1:
            yield nk[0], rel & restr
        else:
            yield nk, rel & restr