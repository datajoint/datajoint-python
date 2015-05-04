# -*- coding: utf-8 -*-
"""
Created on Tue Aug 26 17:42:52 2014

@author: dimitri
"""

import datajoint as dj
import os

print("Welcome to the database 'demo1'")

conn = dj.conn()   # connect to database; conn must be defined in module namespace
conn.bind(module=__name__, dbname='dj_test')  # bind this module to the database


class Subject(dj.Base):
    definition = """
    demo1.Subject (manual)     # Basic subject info
    subject_id       : int     # internal subject id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    date_of_birth=null          : date                          # animal's date of birth
    sex="unknown"               : enum('M','F','unknown')       #
    caretaker="Unknown"         : varchar(20)                   # person responsible for working with this subject
    animal_notes=""             : varchar(4096)                 # strain, genetic manipulations, etc
    """


class Experiment(dj.Base):
    definition = """
    demo1.Experiment (manual)     # Basic subject info
    -> demo1.Subject
    experiment          : smallint   # experiment number for this subject
    ---
    experiment_folder               : varchar(255) # folder path
    experiment_date                 : date        # experiment start date
    experiment_notes=""             : varchar(4096)
    experiment_ts=CURRENT_TIMESTAMP : timestamp   # automatic timestamp
    """


class Session(dj.Base):
    definition = """
    demo1.Session (manual)   # a two-photon imaging session
    -> demo1.Experiment
    session_id    : tinyint  # two-photon session within this experiment
    -----------
    setup      : tinyint   # experimental setup
    lens       : tinyint   # lens e.g.: 10x, 20x, 25x, 60x
    """


class Scan(dj.Base):
    definition = """
    demo1.Scan (manual)   # a two-photon imaging session
    -> demo1.Session
    scan_id : tinyint  # two-photon session within this experiment
    ----
    depth  :   float    #  depth from surface
    wavelength : smallint  # (nm)  laser wavelength
    mwatts: numeric(4,1)  # (mW) laser power to brain
    """

class ScanInfo(dj.Base, dj.AutoPopulate):
    definition = """

    """

    pop_rel = Session

    def make_tuples(self, key):
        info = (Session()*Scan() & key).pro('experiment_folder').fetch()
        filename = os.path.join(info.experiment_folder, 'scan_%03', )


