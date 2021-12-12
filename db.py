from dataclasses import dataclass

import sqlalchemy as sa

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm import sessionmaker


Base: DeclarativeMeta = declarative_base()

class Post(Base):
  """
  school='Carnegie Mellon University (CMU)'
  program='Operation Management, PhD (F21)'
  decision='Interview'
  medium='E-mail'
  date_of_decision='12 Feb 2021'
  status='I'
  date_of_post='26 Feb 2021'
  comment='anyone received offer from this program? had interview on Feb 12 and no updates after that....'
  id=789986

  """
  __tablename__ = 'posts'
  id = sa.Column(sa.Integer, primary_key=True)
  school = sa.Column(sa.String)
  program = sa.Column(sa.String)
  decision = sa.Column(sa.String)
  medium = sa.Column(sa.String)
  date_of_decision = sa.Column(sa.Date)
  status = sa.Column(sa.String)
  date_of_post = sa.Column(sa.Date)
  comment = sa.Column(sa.String)
  gpa = sa.Column(sa.String)
  gre_general = sa.Column(sa.String)
  gre_subject = sa.Column(sa.String)