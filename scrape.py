#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
from typing import List, Optional
from urllib.parse import urlencode
from urllib.request import urlopen

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
    logger.info(f"================== Row #{row_num} ===================")
    tds = list(tr.find_all("td"))
    if len(tds) != NUM_COLUMNS:
      logger.warn(f"Skipping row #{row_num} with {len(tds)} columns (hint: need {NUM_COLUMNS} columns)")
      continue

    try:
      post = table_row_to_post(tds)
      posts.append(post)
    except Exception as e:
      logger.error(f"Got exception when parsing row: {e}")

  return posts

async def main() -> None:
  query = Query().text("cmu").max_num_rows(250)
  data = urlencode(query.to_dict())
  url = f"{API}?{data}"
  with urlopen(url) as f:
    html = f.read().decode('utf-8')

  soup = BeautifulSoup(html, 'lxml')
  posts = soup_to_posts(soup)
  for post in posts or []:
    if not (post_row := session.query(Post).filter(Post.id == post.id).first()):
      session.add(post)

  session.commit()


if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
