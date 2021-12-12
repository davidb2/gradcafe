from __future__ import annotations

import functools

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional, TypeVar

_Self = TypeVar("_Self")
_T = TypeVar("_T")

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
