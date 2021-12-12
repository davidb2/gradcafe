#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import itertools
from argparse import Action, ArgumentParser, Namespace
from typing import Any, List, Optional, Sequence, Union
from urllib.parse import urlencode

import aiohttp
from bs4 import BeautifulSoup
from bs4.element import Tag
from pydantic.env_settings import BaseSettings

from custom_logger import logger
from db import Post, session
from parsing import get_rows, table_row_to_post
from query import Column, Query

NUM_COLUMNS = 6

class Config(BaseSettings):
  api_url: str

def soup_to_posts(soup: BeautifulSoup) -> Optional[List[Post]]:
  table = soup.find(class_="submission-table")
  assert isinstance(table, Tag)

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


async def populate(config: Config, seed: str) -> str:
  query = Query().text(seed).max_num_rows(250)

  async with aiohttp.ClientSession() as http_session:
    pagination_num = 0
    for pagination_num in itertools.count(start=1):
      data = urlencode(query.pagination_num(pagination_num).to_dict())
      url = f"{config.api_url}?{data}"
      async with http_session.get(url) as http_response:
        html = await http_response.text()

      soup = BeautifulSoup(html, 'lxml')

      if (posts := soup_to_posts(soup)) is None:
        break

      new_posts: List[Post] = []
      for post in posts or []:
        if not (post_row := session.query(Post).filter(Post.id == post.id).first()):
          session.add(post)
          new_posts.append(post)

      logger.info(f"Inserting {len(new_posts)} new posts into table, {url=}")
      session.commit()

      if not new_posts:
        break

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
    setattr(namespace, self.dest, [item.strip() for item in items.split(",")])

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
