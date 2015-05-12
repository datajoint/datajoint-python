# -*- coding: utf-8 -*-
"""
Created on Thu Aug 28 00:46:11 2014

@author: dimitri
"""
import logging
import demo1

logging.basicConfig(level=logging.DEBUG)

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

s.insert((3, 'Alice', 'monkey', '2012-09-01'))
s.insert((4, 'Dennis', 'monkey', '2012-09-01'))
s.insert((5, 'Warren', 'monkey', '2012-09-01'))
s.insert((6, 'Franky', 'monkey', '2012-09-01'))
s.insert((7, 'Simon', 'monkey', '2012-09-01', 'F'))
s.insert((8, 'Ferocious', 'monkey', '2012-09-01', 'M'))
s.insert((9, 'Simon', 'monkey', '2012-09-01', 'm'))
s.insert((10, 'Ferocious', 'monkey', '2012-09-01', 'F'))
s.insert((11, 'Simon', 'monkey', '2012-09-01', 'm'))
s.insert((12, 'Ferocious', 'monkey', '2012-09-01', 'M'))
s.insert((13, 'Dauntless', 'monkey', '2012-09-01', 'F'))
s.insert((14, 'Dawn', 'monkey', '2012-09-01', 'F'))

s.insert((12430, 'C0430', 'mouse', '2012-09-01', 'M'))
s.insert((12431, 'C0431', 'mouse', '2012-09-01', 'F'))

print(s)
print(s.project())
print(s.project(name='real_id', dob='date_of_birth', sex='sex') & 'sex="M"')

e.insert(dict(subject_id=1,
              experiment=1,
              experiment_date="2014-08-28",
              experiment_notes="my first experiment"))

e.insert(dict(subject_id=1,
              experiment=2,
              experiment_date="2014-08-28",
              experiment_notes="my second experiment"))

print(e)
print(e*s)
print(s & e)

# cleanup
e.drop()
s.drop()