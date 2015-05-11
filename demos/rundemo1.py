# -*- coding: utf-8 -*-
"""
Created on Thu Aug 28 00:46:11 2014

@author: dimitri
"""

import demo1

s = demo1.Subject()
e = demo1.Experiment()

e.drop()
s.drop()

s.insert(dict(subject_id=1,
              real_id="George",
              species="monkey",
              date_of_birth="2011-01-01",
              sex="M",
              caretaker="Arthur",
              animal_notes="this is a test"))

s.insert(dict(subject_id=2,
              real_id='1373',
              date_of_birth="2014-08-01",
              caretaker="Joe"))

s.insert((3, 'Dennis', 'monkey', '2012-09-01'))
s.insert((12430, 'C0430', 'mouse', '2012-09-01', 'M'))
s.insert((12431, 'C0431', 'mouse', '2012-09-01', 'F'))

print('inserted keys into Subject:')
for tup in s:
    print(tup)

e.insert(dict(subject_id=1,
              experiment=1,
              experiment_date="2014-08-28",
              experiment_notes="my first experiment"))

e.insert(dict(subject_id=1,
              experiment=2,
              experiment_date="2014-08-28",
              experiment_notes="my second experiment"))

# cleanup
e.drop()
s.drop()