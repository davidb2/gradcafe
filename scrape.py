#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import itertools
from argparse import ArgumentParser, Namespace
from typing import List, Optional
from urllib.parse import urlencode

import aiohttp
from bs4 import BeautifulSoup
from bs4.element import Tag

from custom_logger import logger
from db import Post, session
from parsing import get_rows, table_row_to_post
from query import Query

API = 'https://thegradcafe.com/survey/index.php'
NUM_COLUMNS = 6

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

async def main(args: Namespace) -> None:
  seed: str = args.seed
  query = Query().text(seed).max_num_rows(250)

  async with aiohttp.ClientSession() as http_session:
    pagination_num = 0
    for pagination_num in itertools.count(start=1):
      data = urlencode(query.pagination_num(pagination_num).to_dict())
      url = f"{API}?{data}"
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


def get_args() -> Namespace:
  parser = ArgumentParser("scrape gradcafe")
  _ = parser.add_argument("--seed", required=True, type=str, help="the string to search by")

  args = parser.parse_args()
  return args

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main(get_args()))