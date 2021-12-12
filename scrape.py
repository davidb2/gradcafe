#!/usr/bin/env python3
from __future__ import annotations
import argparse
import sys
import asyncio
from typing import Any, Callable, ClassVar, Dict, List, Literal, Match, Optional, Sequence, TypeVar, Union, cast
from urllib import parse
from urllib.request import urlopen
from urllib.parse import urlencode
from enum import Enum
from dataclasses import dataclass
from pathlib import Path
import functools
import re
import datetime as dt

from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag

from db import Post
from query import Query
from parsing import table_row_to_post, get_rows

from custom_logger import logger



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

    post = table_row_to_post(tds)

    input()

    posts.append(post)

  return posts

async def main() -> None:
  query = Query().text("cmu").max_num_rows(50)
  data = urlencode(query.to_dict())
  url = f"{API}?{data}"
  sys.stderr.write(url + "\n")
  with urlopen(url) as f:
    html = f.read().decode('utf-8')

  soup = BeautifulSoup(html, 'lxml')
  posts = soup_to_posts(soup)
  print(posts)


if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
