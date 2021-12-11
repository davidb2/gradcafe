#!/usr/bin/env
from __future__ import annotations
import argparse
import sys
import asyncio
from typing import Any, Callable, ClassVar, Dict, List, Literal, Optional, TypeVar, Union
from urllib.request import urlopen
from urllib.parse import urlencode
from enum import Enum
from dataclasses import dataclass
from pathlib import Path
import functools
import re
import datetime as dt

from bs4 import BeautifulSoup
from bs4.element import Tag

API = 'https://thegradcafe.com/survey/index.php'

class Column(Enum):
  INSTITUTION = 'i'
  """The name of the institution."""
  PROGRAM = 'p'
  """The name of the program."""
  NOTIFICATION_DATE = 'd'
  """The date that the university letter was sent out."""

class QueryParams(Enum):
  """Stores the query key names."""
  TEXT = 'q'
  """The text to search."""
  MAX_NUM_ROWS = "pp"
  """The maximum number of rows to retrieve."""
  PAGINATION_NUM = "p"
  """The current page number of the pagination (one-indexed)."""
  SORT_COLUMN = "o"
  """The column to sort by."""

RowCount = Literal[10]

_Self = TypeVar("_Self")
_T = TypeVar("_T")

def builder(f: Callable[[_Self, _T], None]) -> Callable[[_Self, _T], _Self]:
  def wrapper(self: _Self, param: _T) -> _Self:
    f(self, param)
    return self
  return functools.wraps(f)(wrapper)

@dataclass(init=False)
class Query:
  """Query Builder."""
  _text: Optional[str] = None
  _max_num_rows: Optional[int] = None
  _pagination_num: Optional[int] = None
  _sort_column: Optional[Column] = None

  @builder
  def text(self, text: str):
    self._text = text

  @builder
  def max_num_rows(self, max_num_rows: int):
    self._max_num_rows = max_num_rows

  @builder
  def pagination_num(self, pagination_num: int):
    self._pagination_num = pagination_num

  @builder
  def sort_column(self, sort_column: Column):
    self._sort_column = sort_column

  def to_dict(self) -> Dict[str, Any]:
    query: Dict[QueryParams, Optional[Any]] = {
      QueryParams.TEXT: self._text,
      QueryParams.SORT_COLUMN: self._sort_column,
      QueryParams.PAGINATION_NUM: self._pagination_num,
      QueryParams.MAX_NUM_ROWS: self._max_num_rows,
    }
    return { key.value: value for key, value in query.items() if value is not None }


def get_stats(tag: Tag) -> List[Any]:
  results = []
  acc = []
  for x in tag.contents:
    if isinstance(x, Tag) and x.name == 'br':
      results.append(acc)
      acc = []
    elif isinstance(x, Tag):
      acc.append(x.text.strip())
    else:
      acc.append(x.partition(': ')[-1].strip())

  return results

def is_reply(href):
  return href and href.startswith("/result/")

@dataclass
class Post:
  """
  school='Carnegie Mellon University (CMU)'
  program='Operation Management, PhD (F21)'
  decision='Interview'
  medium='E-mail'
  date_of_decision='12 Feb 2021'
  status='I'
  date_of_post='26 Feb 2021'
  comment='anyone received offer from this program? had interview on Feb 12 and no updates after that....'
  post_id=789986

  """
  school: str
  program: str
  decision: str
  medium: Optional[str]
  date_of_decision: dt.date
  status: str
  date_of_post: dt.date
  comment: Optional[str]
  post_id: int


async def main() -> None:
  query = Query().text("cmu").max_num_rows(50)
  data = urlencode(query.to_dict())
  url = f"{API}?{data}"
  sys.stderr.write(url + "\n")
  with urlopen(url) as f:
    html = f.read().decode('utf-8')

  soup = BeautifulSoup(html, 'lxml')
  table = soup.find(class_="submission-table")
  for row_num, tr in enumerate(table.find_all(lambda tags: any(tag.startswith("row") for tag in tags.get('class', []))), start=1):
    print(f"================== Row #{row_num} =============")
    tds = list(tr.find_all("td"))
    """
    <td class="instcol">Carnegie Mellon University(CMU)</td>
    <td class="">Software Engineering (MSE-SS), Masters (F21)</td>
    <td class="">
      Wait listed via E-mail on 10 Mar 2021
      <a class="extinfo" href="#">
        <span>
          <strong>Undergrad GPA</strong>: 3.06<br/>
          <strong>GRE General (V/Q/W)</strong>: 153/169/3.00<br/>
          <strong>GRE Subject</strong>: n/a<br/>
        </span>
        ♦
      </a>
    </td>
    <td class="">I</td>
    <td class="datecol">10 Mar 2021</td>
    <td class="">
      Did anyone get the status of this program? Who knows how is the chance to turn into an admission from the waitlist for the MSE-SS program?
      <div class="text-end">
        <a class="text-danger controlspam me-2" href="javascript:vote(802698);">report spam</a>
        <a href="/result/802698">reply</a>
      </div>
    </td>
    """

    school = tds[0].text.strip()
    program = tds[1].text.strip()
    decision_medium_and_date = tds[2].find(text=True, recursive=False).strip()
    assert (m := re.match(r'(.*)\s*via\s*(.*)\s*on\s*(.*)', decision_medium_and_date))
    decision = m.group(1).strip()
    medium = m.group(2).strip()
    date_of_decision = m.group(3).strip()
    print(f"{tr.prettify()=!s}")
    print(f"{school=}")
    print(f"{program=}")
    print(f"{decision=}")
    print(f"{medium=}")
    print(f"{date_of_decision=}")
    if span := tds[2].span:
      stats = get_stats(span)
      print(f"{stats=}")
    status = tds[3].text.strip()
    date_of_post = tds[4].text.strip()
    comment_tag = tds[5]
    comment = comment_tag.find(text=True, recursive=False).replace('\r\n', ' ').strip()
    print(f"{status=}")
    print(f"{date_of_post=}")
    print(f"{comment=}")

    reply_tag = comment_tag.find(href=is_reply)
    # print(f"{reply_tag=}, {type(reply_tag)}")
    post_id = int(reply_tag.get("href").partition("/result/")[-1])
    print(f"{post_id=}")
    input()
  # print(table.prettify())



if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
