"""Query expressions"""

import abc


class Expr(metaclass=abc.ABCMeta):
    """The base class for query (sub) expressions"""

    @property
    def __dict__(self) -> dict:
        return self.query()

    def __eq__(self, other):
        return self.query() == other.query()

    @abc.abstractmethod
    def query(self) -> dict:
        """Get the expression as a query dictionary"""

    def __and__(self, other: 'Expr') -> 'And':
        if not isinstance(other, Expr):
            raise TypeError("Expected Expr got '{}'".format(other))
        return And(self, other)

    def __or__(self, other: 'Expr') -> 'Or':
        if not isinstance(other, Expr):
            raise TypeError("Expected Expr got '{}'".format(other))
        return Or(self, other)


class CompoundOper(Expr):
    __slots__ = ('exprs',)
    oper = None  # type: str

    def __init__(self, *exprs):
        self.exprs = exprs

    def query(self) -> dict:
        return {self.oper: [expr.query() for expr in self.exprs]}


class BinaryOper(Expr):
    __slots__ = ('field', 'value')
    oper = None  # type: str

    def __init__(self, field: str, value):
        self.field = field
        self.value = value

    def query(self) -> dict:
        return {self.field: {self.oper: self.value}}


class VariadicOper(Expr):
    """A variadic operator that can take any number of operands"""
    __slots__ = ('operands',)
    oper = None  # type: str

    def __init__(self, field: str, *values):
        if not isinstance(field, str):
            raise TypeError("field must be a string, got '{}'".format(field))

        self.field = field
        self.values = values

    def query(self) -> dict:
        return {self.field: {self.oper: self.values}}


# region Comparison operators


class Eq(BinaryOper):
    __slots__ = ()
    oper = '$eq'

    def query(self) -> dict:
        # Specialise this query as it looks nicer this way (without using '$eq')
        return {self.field: self.value}


class Gt(BinaryOper):
    __slots__ = ()
    oper = '$gt'


class Gte(BinaryOper):
    __slots__ = ()
    oper = '$gte'


class In(VariadicOper):
    __slots__ = ()
    oper = '$in'


class Lt(BinaryOper):
    __slots__ = ()
    oper = '$lt'


class Lte(BinaryOper):
    __slots__ = ()
    oper = '$lte'


class Ne(BinaryOper):
    __slots__ = ()
    oper = '$ne'


class Nin(VariadicOper):
    __slots__ = ()
    oper = '$nin'


# endregion

# region Logical operators


class And(CompoundOper):
    __slots__ = ()
    oper = '$and'

    def __and__(self, other: 'Expr') -> 'And':
        if isinstance(other, And):
            # Economise on Ands and fuse them here
            return And(*[*self.exprs, *other.exprs])

        return super().__and__(other)


class Or(CompoundOper):
    __slots__ = ()
    oper = '$or'

    def __or__(self, other: 'Expr') -> 'Or':
        if isinstance(other, Or):
            # Economise on Ors and fuse them here
            return Or(*[*self.exprs, *other.exprs])

        return super().__or__(other)


class Nor(VariadicOper):
    __slots__ = ()
    oper = '$nor'


# endregion

# region Element operators


class Exists(BinaryOper):
    __slots__ = ()
    oper = '$exists'


# endregion


class Queryable(metaclass=abc.ABCMeta):
    # region Query operations
    __slots__ = ()
    __hash__ = object.__hash__

    def __eq__(self, other) -> Expr:
        return Eq(self._get_path(), other)

    def __ne__(self, other) -> Expr:
        return Ne(self._get_path(), other)

    def __gt__(self, other) -> Expr:
        return Gt(self._get_path(), other)

    def __ge__(self, other) -> Expr:
        return Gte(self._get_path(), other)

    def __lt__(self, other) -> Expr:
        return Lt(self._get_path(), other)

    def __le__(self, other) -> Expr:
        return Lte(self._get_path(), other)

    def in_(self, *possibilities) -> Expr:
        return In(self._get_path(), *possibilities)

    def nin_(self, *possibilities) -> Expr:
        return Nin(self._get_path(), *possibilities)

    def exists_(self, value: bool = True) -> Expr:
        return Exists(self._get_path(), value)

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
        return And(self._query_context, expr)
