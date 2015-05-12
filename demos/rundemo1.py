# -*- coding: utf-8 -*-
"""
Created on Thu Aug 28 00:46:11 2014

@author: dimitri
"""
import logging
import demo1

logging.basicConfig(level=logging.DEBUG)

subject = demo1.Subject()
experiment = demo1.Experiment()
session = demo1.Session()
scan = demo1.Scan()

scan.drop()
session.drop()
experiment.drop()
subject.drop()

subject.insert(dict(subject_id=1,
                    real_id="George",
                    species="monkey",
                    date_of_birth="2011-01-01",
                    sex="M",
                    caretaker="Arthur",
                    animal_notes="this is a test"))

subject.insert(dict(subject_id=2,
                    real_id='1373',
                    date_of_birth="2014-08-01",
                    caretaker="Joe"))

subject.insert((3, 'Alice', 'monkey', '2012-09-01'))
subject.insert((4, 'Dennis', 'monkey', '2012-09-01'))
subject.insert((5, 'Warren', 'monkey', '2012-09-01'))
subject.insert((6, 'Franky', 'monkey', '2012-09-01'))
subject.insert((7, 'Simon', 'monkey', '2012-09-01', 'F'))
subject.insert((8, 'Ferocious', 'monkey', '2012-09-01', 'M'))
subject.insert((9, 'Simon', 'monkey', '2012-09-01', 'm'))
subject.insert((10, 'Ferocious', 'monkey', '2012-09-01', 'F'))
subject.insert((11, 'Simon', 'monkey', '2012-09-01', 'm'))
subject.insert((12, 'Ferocious', 'monkey', '2012-09-01', 'M'))
subject.insert((13, 'Dauntless', 'monkey', '2012-09-01', 'F'))
subject.insert((14, 'Dawn', 'monkey', '2012-09-01', 'F'))

subject.insert((12430, 'C0430', 'mouse', '2012-09-01', 'M'))
subject.insert((12431, 'C0431', 'mouse', '2012-09-01', 'F'))

print(subject)
print(subject.project())
print(subject.project(name='real_id', dob='date_of_birth', sex='sex') & 'sex="M"')

(subject & dict(subject_id=12431)).delete()
print(subject)

experiment.insert(dict(
    subject_id=1,
    experiment=1,
    experiment_date="2014-08-28",
    experiment_notes="my first experiment"))

experiment.insert(dict(
    subject_id=1,
    experiment=2,
    experiment_date="2014-08-28",
    experiment_notes="my second experiment"))

experiment.insert(dict(
    subject_id=2,
    experiment=1,
    experiment_date="2015-05-01"
))

print(experiment)
print(experiment * subject)
print(subject & experiment)
print(subject - experiment)

session.insert(dict(
    subject_id=1,
    experiment=2,
    session_id=1,
    setup=0,
    lens="20x"
))

scan.insert(dict(
    subject_id=1,
    experiment=2,
    session_id=1,
    scan_id=1,
    depth=250,
    wavelength=980,
    mwatts=30.5
))

print((scan * experiment) % ('wavelength->lambda', 'experiment_date'))

# cleanup
scan.drop()
session.drop()
experiment.drop()
subject.drop()