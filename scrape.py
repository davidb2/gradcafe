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

from custom_logger import logger
from db import Post, session
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

async def get_soup(request: ScrapeRequest) -> BeautifulSoup:
    data = urlencode(request.query.pagination_num(request.pagination_num).to_dict())
    url = f"{request.config.api_url}?{data}"
    async with request.http_session.get(url) as http_response:
      html = await http_response.text()

    soup = BeautifulSoup(html, 'lxml')
    return soup

async def get_counts(request: ScrapeRequest) -> Optional[Counts]:
  try:
    soup = await get_soup(request)
  except Exception as e:
    logger.error(f"Got exception while getting soup: {e}")
    return None

  return Parser.parse_counts(soup)

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

  # Try to batch insert.
  new_posts: List[Post] = []
  for post in posts or []:
    try:
      if not (post_row := session.query(Post).filter(Post.id == post.id).first()):
        session.add(post)
        new_posts.append(post)
    except Exception as e:
      logger.error(f"Got exception when adding/fetching row: {e}")

  try:
    session.commit()
    logger.debug(f"Inserted {len(new_posts)} new posts into table, ({request.seed=}, {request.pagination_num=})")
    return
  except Exception as e:
    logger.error(f"Got error while trying to commit: {e}.\n Falling back on individual commits.")
    logger.info(f"Falling back on individual commits")
    session.rollback()

  # Fall back on committing individually.
  for post in new_posts:
    try:
      session.add(post)
      session.commit()
    except Exception as e:
      logger.error(f"Error committing individual post {post.id=}: {e}")


async def populate(config: Config, seed: str) -> str:
  query = Query().text(seed).max_num_rows(250)

  async with ClientSession() as http_session:
    request = ScrapeRequest(config, seed, http_session, query, 0)
    if not (counts := await get_counts(request)):
      return seed

    logger.info(f"{seed=}, {counts=}")
    pages = [
      scrape(ScrapeRequest(config, seed, http_session, query, pagination_num))
      for pagination_num in range(1, counts.pages+1)
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
