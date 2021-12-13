#!/usr/bin/env python3
from __future__ import annotations
from dataclasses import dataclass
import math
from typing import Callable, List, Match, Optional, Sequence, TypeVar, cast
import functools
import re
from bs4 import BeautifulSoup
import datetime as dt

from bs4.element import ResultSet, Tag

from db import Post

from custom_logger import logger


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

_T = TypeVar("_T")
_InputType = TypeVar("_InputType")
_ReturnType = TypeVar("_ReturnType")

@dataclass
class Counts:
  count: int
  pages: int

def is_post_row(tag: Tag) -> bool:
  classes: Sequence[str] = tag.get('class') or []
  return any(class_.startswith("row") for class_ in classes)

def get_rows(table: Tag) -> ResultSet[Tag]:
  return table.find_all(is_post_row)

def graceful(sf: Callable[[_InputType], _ReturnType]) -> Callable[[_InputType], Optional[_ReturnType]]:
  f = cast(Callable[[_InputType], _ReturnType], sf.__func__) # type: ignore
  def wrapper(parser_input: _InputType) -> Optional[_ReturnType]:
    try:
      return f(parser_input)
    except Exception as e:
      logger.error(f"Failed to parse {f.__name__}: {e}")
      return None
  return functools.wraps(f)(wrapper)

class Parser:
  """Parsing methods."""
  @graceful
  @staticmethod
  def parse_school(td: Tag) -> Optional[str]:
    return td.text.strip() or None

  @graceful
  @staticmethod
  def parse_progam(td: Tag) -> Optional[str]:
    return td.text.strip() or None

  @graceful
  @staticmethod
  def parse_decision_medium_and_date(td: Tag) -> Optional[Match[str]]:
    tokens: List[str] = []
    if (possible_decision := td.find("strong", recursive=False)):
      tokens.append(possible_decision.text)

    if (possible_decision_medium_and_date := td.find(text=True, recursive=False)):
      tokens.append(possible_decision_medium_and_date.text)

    decision_medium_and_date = (' '.join(tokens)).strip()
    return re.match(r'(.*)\s*via\s*(.*)\s*on\s*(.*)', decision_medium_and_date)

  @graceful
  @staticmethod
  def parse_decision(match: Match[str]) -> Optional[str]:
    return match.group(1).strip() or None

  @graceful
  @staticmethod
  def parse_medium(match: Match[str]) -> Optional[str]:
    return match.group(2).strip() or None

  @graceful
  @staticmethod
  def parse_date_of_decision(match: Match[str]) -> Optional[dt.date]:
    if not (date_str := match.group(3).strip()):
      return None
    return dt.datetime.strptime(date_str, "%d %b %Y").date()

  @graceful
  @staticmethod
  def parse_stats(td: Tag) -> Optional[List[List[str]]]:
    if not (span := td.span): return None

    stats = get_stats(span)

    if len(stats) != 3:
      logger.warn(f"we see weird stat format: {stats}")

    return stats

  @graceful
  @staticmethod
  def parse_gpa(stats: List[List[str]]) -> Optional[float]:
    if not (text := stats[0][1].strip()):
      return None

    if text == 'n/a':
      return None

    return float(text)

  @graceful
  @staticmethod
  def parse_gre_general(stats: List[List[str]]) -> Optional[Match[str]]:
    if not (text := stats[1][1].strip()):
      return None

    return re.match(r'(.*)/(.*)/(.*)', text)

  @graceful
  @staticmethod
  def parse_gre_verbal(match: Match[str]) -> Optional[float]:
    if math.isclose(verbal := float(match.group(1).strip()), 0):
      return None

    return verbal

  @graceful
  @staticmethod
  def parse_gre_quant(match: Match[str]) -> Optional[float]:
    if math.isclose(quant := float(match.group(2).strip()), 0):
      return None

    return quant

  @graceful
  @staticmethod
  def parse_gre_writing(match: Match[str]) -> Optional[float]:
    if math.isclose(writing := float(match.group(3).strip()), 0):
      return None

    return writing

  @graceful
  @staticmethod
  def parse_gre_subject(stats: List[List[str]]) -> Optional[str]:
    if not (text := stats[2][1].strip()):
      return None

    if text == 'n/a':
      return None

    return text

  @graceful
  @staticmethod
  def parse_status(td: Tag) -> Optional[str]:
    return td.text.strip() or None

  @graceful
  @staticmethod
  def parse_date_of_post(td: Tag) -> Optional[dt.date]:
    if not (date_str := td.text.strip()):
      return None
    return dt.datetime.strptime(date_str, "%d %b %Y").date()

  @graceful
  @staticmethod
  def parse_comment(td: Tag) -> Optional[str]:
    if (possible_comment := td.find(text=True, recursive=False)) is None:
      return None

    return cast(str, possible_comment).replace('\r\n', ' ').strip()

  @graceful
  @staticmethod
  def parse_id(td: Tag) -> Optional[int]:
    if (reply_tag := td.find(href=is_reply)) is None or (href := cast(Tag, reply_tag).get("href")) is None:
      return None

    return int(cast(str, href).partition("/result/")[-1])

  @graceful
  @staticmethod
  def parse_counts(soup: BeautifulSoup) -> Optional[Counts]:
    """
    <div class="col-auto align-self-center pe-3">
      Showing <strong>497</strong> results over <strong>20</strong> pages
    </div>
    """
    if not (text := soup.find(text=re.compile(r'\s*Showing'))):
      return None

    if not isinstance(div := text.parent, Tag):
      return None

    if not (match := re.match(r'\s*Showing\s*(.*)\s*results\s*over\s*(.*)\s*pages\s*', div.text)):
      return None

    if not (count_str := match.group(1)) or not (pages_str := match.group(2)):
      return None

    return Counts(
      count=int(count_str.replace(",", "")),
      pages=int(pages_str.replace(",", "")),
    )

def get_stats(tag: Tag) -> List[List[str]]:
  results: List[List[str]] = []
  acc: List[str] = []
  for x in tag.contents:
    if isinstance(x, Tag) and x.name == 'br':
      results.append(acc)
      acc = []
    elif isinstance(x, Tag):
      acc.append(x.text.strip())
    else:
      acc.append(cast(str, x).partition(': ')[-1].strip())

  return results

def is_reply(href: str) -> bool:
  return bool(href) and href.startswith("/result/")

def table_row_to_post(tds: List[Tag]) -> Post:
  school = Parser.parse_school(tds[0])
  program = Parser.parse_progam(tds[1])

  logger.debug(f"{school=}")
  logger.debug(f"{program=}")

  decision: Optional[str] = None
  medium: Optional[str] = None
  date_of_decision: Optional[dt.date] = None
  if match := Parser.parse_decision_medium_and_date(tds[2]):
    decision = Parser.parse_decision(match)
    medium = Parser.parse_medium(match)
    date_of_decision = Parser.parse_date_of_decision(match)

    logger.debug(f"{decision=}")
    logger.debug(f"{medium=}")
    logger.debug(f"{date_of_decision=}")

  gpa: Optional[float] = None
  gre_subject: Optional[str] = None
  gre_verbal: Optional[float] = None
  gre_quant: Optional[float] = None
  gre_writing: Optional[float] = None
  if stats := Parser.parse_stats(tds[2]):
    gpa = Parser.parse_gpa(stats)
    gre_subject = Parser.parse_gre_subject(stats)
    logger.debug(f"{gpa=}")
    logger.debug(f"{gre_subject=}")

    if gre_general := Parser.parse_gre_general(stats):
      gre_verbal = Parser.parse_gre_verbal(gre_general)
      gre_quant = Parser.parse_gre_quant(gre_general)
      gre_writing = Parser.parse_gre_writing(gre_general)

      logger.debug(f"{gre_verbal=}")
      logger.debug(f"{gre_quant=}")
      logger.debug(f"{gre_writing=}")

  status = Parser.parse_status(tds[3])
  date_of_post = Parser.parse_date_of_post(tds[4])
  comment = Parser.parse_comment(tds[5])

  logger.debug(f"{status=}")
  logger.debug(f"{date_of_post=}")
  logger.debug(f"{comment=}")

  post_id = Parser.parse_id(tds[5])
  logger.debug(f"{post_id=}")

  return Post(
    id=post_id,
    decision=decision,
    medium=medium,
    date_of_decision=date_of_decision,
    date_of_post=date_of_post,
    school=school,
    status=status,
    program=program,
    comment=comment,
    gpa=gpa,
    gre_verbal=gre_verbal,
    gre_quant=gre_quant,
    gre_writing=gre_writing,
    gre_subject=gre_subject,
  )