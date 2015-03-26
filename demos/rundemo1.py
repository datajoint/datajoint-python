# -*- coding: utf-8 -*-
"""
Created on Thu Aug 28 00:46:11 2014

@author: dimitri
"""

import demo1

s = demo1.Subject()
# insert as dict
s.insert(dict(subject_id=1,
              real_id='George',
              species="monkey",
              date_of_birth="2011-01-01",
              sex="M",
              caretaker="Arthur",
              animal_notes="this is a test"))
s.insert(dict(subject_id=2,
              real_id='1373',
              date_of_birth="2014-08-01",
              caretaker="Joe"))
# insert as tuple. Attributes must be in the same order as in table declaration
s.insert((3,'Dennis','monkey','2012-09-01'))

# TODO: insert as ndarray


print('inserted keys into Subject:')
for key in s:
    print(key)


#
e = demo1.Experiment()
e.insert(dict(subject_id=1,experiment=1,experiment_date="2014-08-28",experiment_notes="my first experiment"))
e.insert(dict(subject_id=1,experiment=2,experiment_date="2014-08-28",experiment_notes="my second experiment"))


# drop the tables
#s.drop
#e.drop
