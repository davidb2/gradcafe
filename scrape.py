#!/usr/bin/env python3
from __future__ import annotations

import asyncio
from argparse import Action, ArgumentParser, Namespace
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Union
from urllib.parse import urlencode

from aiohttp.client import ClientSession
from bs4 import BeautifulSoup
from bs4.element import Tag
from pydantic.env_settings import BaseSettings
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import insert, select

from custom_logger import logger
from db import Post, Sessions
from parsing import Counts, Parser, get_rows, table_row_to_post
from query import Query

NUM_COLUMNS = 6

class Config(BaseSettings):
  api_url: str

def soup_to_posts(soup: BeautifulSoup) -> Optional[List[Post]]:
  table = soup.find(class_="submission-table")
  if not isinstance(table, Tag):
    return None

  if not (rows := get_rows(table)):
    return None

  posts: List[Post] = []
  for row_num, tr in enumerate(rows, start=1):
    logger.debug(f"================== Row #{row_num} ===================")
    tds: List[Tag] = list(tr.find_all("td"))
    if len(tds) != NUM_COLUMNS:
      logger.warn(f"Skipping row #{row_num} with {len(tds)} columns (hint: need {NUM_COLUMNS} columns)")
      continue

    try:
      post = table_row_to_post(tds)
      posts.append(post)
    except Exception as e:
      logger.error(f"Got exception when parsing row: {e}")

  return posts

@dataclass
class ScrapeRequest:
  config: Config
  seed: str
  http_session: ClientSession
  query: Query
  pagination_num: int
  session: AsyncSession

async def get_soup(request: ScrapeRequest) -> BeautifulSoup:
    data = urlencode(request.query.pagination_num(request.pagination_num).to_dict())
    url = f"{request.config.api_url}?{data}"
    async with request.http_session.get(url) as http_response:
      try:
        html = await http_response.text(errors='replace')
      except Exception as e:
        logger.error(f"html parsing problem for {url=}")
        raise e

      return BeautifulSoup(html, 'lxml')

async def get_counts(request: ScrapeRequest) -> Optional[Counts]:
  try:
    soup = await get_soup(request)
  except Exception as e:
    logger.error(f"Got exception while getting soup: {e}")
    return None

  return Parser.parse_counts(soup)


@dataclass
class SessionHelper:
  session: AsyncSession

  async def exists(self, post: Post) -> bool:
    try:
      result = await self.session.get(Post, post.id)
      return bool(result)
    except Exception as e:
      logger.error(f"Got exception while querying database: {e}")
    return True

  async def add(self, post: Post, *, commit: bool = False) -> None:
    try:
      self.session.add_all([post]) # type: ignore
      if commit:
        await self.session.commit()
    except Exception as e:
      logger.error(f"Got exception when adding/fetching row: {e}")
      await self.session.rollback()

  async def commit_batch(self) -> bool:
    try:
      await self.session.commit()
      logger.debug(f"Inserted new posts into table")
      return True
    except Exception as e:
      logger.error(f"Got error while trying to commit: {e}.\n Falling back on individual commits.")
      logger.info(f"Falling back on individual commits")
      await self.session.rollback()
    return False
  

async def scrape(request: ScrapeRequest):
  try:
    soup = await get_soup(request)
  except Exception as e:
    logger.error(f"Got exception while getting soup: {e}")
    return

  try:
    posts = soup_to_posts(soup)
  except Exception as e:
    logger.error(f"Got error when converting soup to posts: {e}")
    return

  helper = SessionHelper(request.session)
  # Try to batch insert.
  new_posts: List[Post] = []
  for post in posts or []:
    if not await helper.exists(post):
      new_posts.append(post)

  for post in new_posts:
    await helper.add(post)
  else:
    if await helper.commit_batch():
      return

  # Fall back on committing individually.
  for post in new_posts:
    _ = await helper.add(post, commit=True)


async def populate(config: Config, seed: str) -> Optional[str]:
  query = Query().text(seed).max_num_rows(250)

  sessions = Sessions()
  async with ClientSession() as http_session:
    if not (session := await sessions.get_session()):
      return None

    request = ScrapeRequest(config, seed, http_session, query, 0, session)
    if not (counts := await get_counts(request)):
      return seed

    logger.info(f"{seed=}, {counts=}")
    pages = [
      scrape(ScrapeRequest(config, seed, http_session, query, pagination_num, session))
      for pagination_num in range(1, counts.pages+1)
      if (session := await sessions.get_session())
    ]

    for page in asyncio.as_completed(pages):
      await page

  return seed


async def main(args: Namespace) -> None:
  seeds: List[str] = args.seeds
  config = Config()

  results = [populate(config, seed) for seed in seeds]
  for result in asyncio.as_completed(results):
    seed = await result
    logger.info(f"finished scraping {seed=}")


class SplitArgs(Action):
  def __call__(
    self,
    parser: ArgumentParser,
    namespace: Namespace,
    values: Optional[Union[str, Sequence[Any]]],
    option_string: Optional[str] = None,
  ):
    logger.info(f"{namespace=}, {values=}, {option_string=}")
    assert isinstance(values, list)
    assert len(values) == 1
    items: str = values[0]
    setattr(namespace, self.dest, [stripped_item for item in items.split(",") if (stripped_item := item.strip())])

def get_args() -> Namespace:
  parser = ArgumentParser("scrape gradcafe")
  _ = parser.add_argument(
    "--seeds",
    dest="seeds",
    required=True,
    type=str,
    action=SplitArgs,
    nargs="+",
    default=[],
    help="the string to search by",
  )

  args = parser.parse_args()
  logger.info(f"{args=}")
  return args

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main(get_args()))
