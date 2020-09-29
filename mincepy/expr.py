"""Query expressions"""
import abc
import copy
from typing import Union, List, Iterable


class FilterLike(metaclass=abc.ABCMeta):
    """An abstract base class for objects representing a pyos path, e.g. pyos.pathlib.PurePath."""

    # pylint: disable=too-few-public-methods

    @abc.abstractmethod
    def __query_expr__(self) -> dict:
        """Return the query filter representation of the object."""


FilterSpec = Union[dict, FilterLike]


class Expr(FilterLike, metaclass=abc.ABCMeta):
    """The base class for query expressions.  Expressions are tuples containing an operator or a
    field as a first part and a value or expression as second"""
    __slots__ = ()

    def dict(self):
        """Return the query dictionary for this expression"""
        return self.__query_expr__()

    def __and__(self, other: 'Expr') -> 'And':
        if not isinstance(other, Expr):
            raise TypeError("Expected Expr got '{}'".format(other))
        return And([self, other])

    def __or__(self, other: 'Expr') -> 'Or':
        if not isinstance(other, Expr):
            raise TypeError("Expected Expr got '{}'".format(other))
        return Or([self, other])


class WithListOperand(FilterLike):
    """Mixin for expressions that take an operand that is a list"""

    # pylint: disable=no-member, too-few-public-methods

    def __init__(self, operand: List[Expr]):
        if not isinstance(operand, list):
            raise TypeError("Expected a list, got {}".format(type(operand).__name__))
        for entry in operand:
            if not isinstance(entry, Expr):
                raise TypeError("Expected a list of Expr, found {}".format(type(entry).__name__))
        super().__init__(operand)

    def __query_expr__(self) -> dict:
        if len(self.operand) == 1:
            return query_expr(self.operand[0])

        return {self.oper: list(map(query_expr, self.operand))}


class Empty(Expr):
    """The empty expression"""

    def __query_expr__(self) -> dict:
        return {}


# region Match


class OperatorExpr(Expr):
    """An operator expression.

    Consists of an operator applied to an operand which is to be matched
    """

    __slots__ = 'field', 'value'
    oper = None  # type: str

    def __init__(self, value):
        self.value = value

    def __query_expr__(self) -> dict:
        return {self.oper: self.value}


class Eq(OperatorExpr):
    __slots__ = ()
    oper = '$eq'


class Gt(OperatorExpr):
    __slots__ = ()
    oper = '$gt'


class Gte(OperatorExpr):
    __slots__ = ()
    oper = '$gte'


class In(OperatorExpr):
    __slots__ = ()
    oper = '$in'


class Lt(OperatorExpr):
    __slots__ = ()
    oper = '$lt'


class Lte(OperatorExpr):
    __slots__ = ()
    oper = '$lte'


class Ne(OperatorExpr):
    __slots__ = ()
    oper = '$ne'


class Nin(OperatorExpr):
    __slots__ = ()
    oper = '$nin'


class Match(Expr):
    """A match expression consists of a field and an operator expression e.g. name == 'frank'
    where name is the field, the operator is ==, and the value is 'frank'
    """
    __slots__ = 'field', 'expr'

    def __init__(self, field, expr: OperatorExpr):
        if not isinstance(expr, OperatorExpr):
            raise TypeError("Expected an operator expression, got '{}'".format(type(expr).__name__))

        self.field = field
        self.expr = expr

    def __query_expr__(self) -> dict:
        if isinstance(self.expr, Eq):
            # Special case for this query as it looks nicer this way (without using '$eq')
            return {self.field: self.expr.value}

        return {self.field: query_expr(self.expr)}


# endregion

# region Logical operators


class LogicalOper(Expr):
    """A comparison operation.  Consists of an operator applied to an operand which is matched in a
    particular way"""

    __slots__ = ('expr',)
    oper = None  # type: str

    def __init__(self, operand):
        self.operand = operand

    def __query_expr__(self) -> dict:
        return {self.oper: self.operand}


class And(WithListOperand, LogicalOper):
    __slots__ = ()
    oper = '$and'

    def __and__(self, other: 'Expr') -> 'And':
        if isinstance(other, And):
            # Economise on Ands and fuse them here
            return And([*self.operand, *other.operand])

        return super().__and__(other)


class Not(LogicalOper):
    __slots__ = ()
    oper = '$not'


class Or(WithListOperand, LogicalOper):
    __slots__ = ()
    oper = '$or'

    def __or__(self, other: 'Expr') -> 'Or':
        if isinstance(other, Or):
            # Economise on Ors and fuse them here
            return Or([*self.operand, *other.operand])

        return super().__or__(other)


class Nor(WithListOperand, LogicalOper):
    __slots__ = ()
    oper = '$nor'


# endregion

# region Element operators


class Exists(OperatorExpr):
    __slots__ = ()
    oper = '$exists'


# endregion


class Queryable(metaclass=abc.ABCMeta):
    # region Query operations
    __slots__ = ()
    __hash__ = object.__hash__

    def __eq__(self, other) -> Match:
        return Match(self._get_path(), Eq(other))

    def __ne__(self, other) -> Match:
        return Match(self._get_path(), Ne(other))

    def __gt__(self, other) -> Match:
        return Match(self._get_path(), Gt(other))

    def __ge__(self, other) -> Match:
        return Match(self._get_path(), Gte(other))

    def __lt__(self, other) -> Match:
        return Match(self._get_path(), Lt(other))

    def __le__(self, other) -> Match:
        return Match(self._get_path(), Lte(other))

    def in_(self, *possibilities) -> Match:
        return Match(self._get_path(), In(possibilities))

    def nin_(self, *possibilities) -> Expr:
        return Match(self._get_path(), Nin(possibilities))

    def exists_(self, value: bool = True) -> Expr:
        return Match(self._get_path(), Exists(value))

    @abc.abstractmethod
    def _get_path(self) -> str:
        """Get the path for this object in the document"""


class WithQueryContext:
    """A mixin for Queryable objects that allows a context to be added which is always 'anded' with
    the resulting query condition for any operator"""
    _query_context = None

    # pylint: disable=no-member

    def set_query_context(self, expr: Expr):
        self._query_context = expr

    def __eq__(self, other) -> Expr:
        return self._combine(super().__eq__(other))

    def __ne__(self, other) -> Expr:
        return self._combine(super().__ne__(other))

    def __gt__(self, other) -> Expr:
        return self._combine(super().__gt__(other))

    def __ge__(self, other) -> Expr:
        return self._combine(super().__ge__(other))

    def __lt__(self, other) -> Expr:
        return self._combine(super().__lt__(other))

    def __le__(self, other) -> Expr:
        return self._combine(super().__le__(other))

    def in_(self, *possibilities) -> Expr:
        return self._combine(super().in_(*possibilities))

    def nin_(self, *possibilities) -> Expr:
        return self._combine(super().nin_(*possibilities))

    def exists_(self, value: bool = True) -> Expr:
        return self._combine(super().exists_(value))

    def _combine(self, expr: Expr) -> Expr:
        if self._query_context is None:
            return expr
        return And([self._query_context, expr])


def query_expr(filter: FilterLike) -> dict:  # pylint: disable=redefined-builtin
    """Return a query specification (dict)

    If a dict is passed is is returned unaltered.
    Otherwise __qspec__() is called and its value is returned as long as it is a dict. In all other
    cases, TypeError is raised."""
    if isinstance(filter, dict):
        return filter

    # Work from the object's type to match method resolution of other magic methods.
    try:
        query_repr = filter.__query_expr__()
    except AttributeError:
        raise TypeError("expected dict or object with __query_expr__, not " + str(filter)) from None

    if isinstance(query_repr, dict):
        return query_repr

    raise TypeError("expected {}.__query_expr__() to return dict, not {}".format(
        type(filter).__name__,
        type(query_repr).__name__))


def build_expr(item) -> Expr:
    """Expression factory"""
    # pylint: disable=too-many-branches, too-many-return-statements

    if isinstance(item, Expr):
        return item

    if isinstance(item, dict):
        if not item:
            return Empty()

        if len(item) == 1:
            return build_expr(tuple(item.items())[0])

        # Otherwise a dictionary is an implicit 'and'
        return And(list(map(build_expr, item.items())))

    if isinstance(item, tuple):
        if len(item) != 2:
            raise ValueError("Expecting tuple of length two, instead got {}".format(item))

        first, second = item
        if first.startswith('$'):
            # Comparison operators
            if first == '$eq':
                return Eq(second)
            if first == '$gt':
                return Gt(second)
            if first == '$gte':
                return Gte(second)
            if first == '$in':
                return In(second)
            if first == '$lt':
                return Lt(second)
            if first == '$lte':
                return Lte(second)
            if first == '$ne':
                return Ne(second)
            if first == '$nin':
                return Nin(second)
            # Logical operators
            if first == '$and':
                return And(list(map(build_expr, second)))
            if first == '$not':
                return Not(build_expr(second))
            if first == '$nor':
                return Nor(list(map(build_expr, second)))
            if first == '$or':
                return Or(list(map(build_expr, second)))
            # Element query
            if first == '$exists':
                return Exists(second)

            raise ValueError("Unknown operator '{}'".format(item))
        else:
            # Must be a 'match' where the first is the field
            try:
                return Match(first, build_expr(second))
            except (ValueError, TypeError):
                # TODO: See if we can make this check safer
                # Assume second is a value type
                return Match(first, Eq(second))

    try:
        return item.__expr__()
    except AttributeError:
        raise TypeError("expected dict or object with __expr__, not " +
                        type(item).__name__) from None


class Query:
    __slots__ = '_filter_expressions', 'limit', 'sort', 'skip'

    def __init__(self, *expr: Expr, limit: int = None, sort: dict = None, skip: int = None):
        self._filter_expressions = []  # type: List[Expr]
        self.extend(expr)
        self.limit = limit
        self.sort = sort
        self.skip = skip

    def __str__(self) -> str:
        return str(self.__dict__)

    def copy(self) -> 'Query':
        return Query(*copy.deepcopy(self._filter_expressions),
                     limit=self.limit,
                     sort=self.sort,
                     skip=self.skip)

    @property
    def __dict__(self) -> dict:
        return dict(filter=self.get_filter(), sort=self.sort, limit=self.limit, skip=self.skip)

    def append(self, expr: Expr):
        self._filter_expressions.append(build_expr(expr))

    def extend(self, exprs: Iterable[Expr]):
        for entry in exprs:
            self.append(entry)

    def get_filter(self) -> dict:
        """Get the query filter as a dictionary"""
        # TODO: This will overwrite parts of the dictionary where the keys match.  We should detect
        # TODO: such cases and combine them with an 'and' expression
        my_filter = {}
        for entry in self._filter_expressions:
            my_filter.update(query_expr(entry))
        return my_filter
