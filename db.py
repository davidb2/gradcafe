from pydantic.types import SecretStr

import sqlalchemy as sa

from pydantic import BaseSettings
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session



Base: DeclarativeMeta = declarative_base()

class Post(Base): # type: ignore
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
  gpa = sa.Column(sa.Numeric)
  gre_verbal = sa.Column(sa.Numeric)
  gre_quant = sa.Column(sa.Numeric)
  gre_writing = sa.Column(sa.Numeric)
  gre_subject = sa.Column(sa.String)

class DBSettings(BaseSettings):
  class Config: # type: ignore
    env_prefix = 'db_'
    env_file = '.env'
    env_file_encoding = 'utf-8'

  host: str
  name: str
  password: SecretStr
  port: int
  user: str

  @property
  def dsn(self) -> str:
    return f"postgresql://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.name}"

db_settings = DBSettings()
engine = sa.create_engine(db_settings.dsn, echo=False)
Base.metadata.create_all(engine) # type: ignore
_Session = sessionmaker(bind=engine)
session: Session = _Session()