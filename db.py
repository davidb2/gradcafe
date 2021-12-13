import asyncio
from typing import Optional
from pydantic.types import SecretStr

import sqlalchemy as sa
import datetime as dt

from pydantic import BaseSettings
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from custom_logger import logger


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
    return f"postgresql+asyncpg://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.name}"

class Sessions:
  _initialized: bool
  _engine: AsyncEngine
  _DBSession: Optional[sessionmaker]

  def __init__(self):
    self._initialized = False
    self._db_settings = DBSettings()
    self._DBSession = None
    self._engine = create_async_engine(
      self._db_settings.dsn,
      echo=False,
      pool_size=5,
      max_overflow=5,
      pool_timeout=dt.timedelta(minutes=2).total_seconds(),
    )

    self._init_task = asyncio.create_task(self._initialize())
  
  async def _initialize(self) -> bool:
    assert not self._initialized
    async with self._engine.begin() as conn:
      if not conn:
        logger.critical(f"could not create engine")
        return False
      await conn.run_sync(Base.metadata.create_all)  # type: ignore
    self._DBSession = sessionmaker(bind=self._engine, expire_on_commit=False, class_=AsyncSession)
    self._initialized = True
    return True

  async def get_session(self) -> Optional[AsyncSession]:
    if not self._init_task.done():
      await self._init_task
    return self._DBSession() if self._DBSession else None
