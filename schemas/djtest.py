# -*- coding: utf-8 -*-
"""
Created on Tue Aug 26 17:42:52 2014

@author: dimitri
"""

import datajoint as dj

print("Welcome to the database 'test'")

conn = dj.conn()   # connect to database
conn.bind(__name__, 'dimitri_test')  # bind this module to the database



class Subject(dj.Base):
    """
    djtest.Subject (manual)     # Basic subject info
    subject_id       : int     # internal id  
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species     
    date_of_birth=null          : date                          # animal's date of birth
    sex="unknown"               : enum('M','F','unknown')       #
    caretaker="Unknown"         : varchar(20)                   # person responsible for working with this subject
    animal_notes=null           : varchar(4096)                 # strain, genetic manipulations, etc
    subject_ts=CURRENT_TIMESTAMP: timestamp                     # automatic timestamp
    """



class Experiment(dj.Base):
    """    
    djtest.Experiment (manual)     # Basic subject info
    -> djtest.Subject         
    experiment          : smallint   # internal id  '
    ---
    experiment_date                 : date        # experiment start
    experiment_ts=CURRENT_TIMESTAMP : timestamp   # automatic timestamp
    """


