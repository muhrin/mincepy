from collections.abc import Hashable
from typing import Union

TypeId = Hashable
TypeIdOrType = Union[TypeId, type]  # pylint: disable=invalid-name
