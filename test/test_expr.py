# -*- coding: utf-8 -*-
import pytest

from mincepy import expr

# pylint: disable=pointless-statement, invalid-name


def test_expr_types_and_filters():
    """Test the query filters for expressions"""
    name_eq = expr.Comparison("name", expr.Eq("frank"))
    age_gt = expr.Comparison("age", expr.Gt(38))

    # Check all operators
    for expression in expr.SimpleOperator.__subclasses__():
        assert expression.oper.startswith("$")
        assert expression(True).dict() == {expression.oper: True}

    # Check all logicals
    for list_expr in expr.Logical.__subclasses__():
        assert list_expr.oper.startswith("$")
        if issubclass(list_expr, expr.WithListOperand):
            assert list_expr([name_eq, age_gt]).dict() == {
                list_expr.oper: [name_eq.dict(), age_gt.dict()]
            }

            # Check that the the passthrough for list expressions work
            assert list_expr([name_eq]).dict() == name_eq.dict()

            with pytest.raises(TypeError):
                list_expr("non list")
            with pytest.raises(TypeError):
                list_expr(["non expression"])
        else:
            assert list_expr(name_eq).dict() == {list_expr.oper: name_eq.dict()}
            with pytest.raises(TypeError):
                list_expr(["non expression"])

    assert expr.Empty().dict() == {}

    with pytest.raises(TypeError):
        # Comparison takes an operator, not a string
        expr.Comparison("my_field", "oper")


def test_expr_and_or():
    """Test expression and/or methods"""
    name_eq = expr.Comparison("name", expr.Eq("frank"))
    age_gt = expr.Comparison("age", expr.Gt(38))

    anded = name_eq & age_gt
    assert anded.dict() == {"$and": [name_eq.dict(), age_gt.dict()]}

    ored = name_eq | age_gt
    assert ored.dict() == {"$or": [name_eq.dict(), age_gt.dict()]}

    with pytest.raises(TypeError):
        name_eq & "hello"

    with pytest.raises(TypeError):
        name_eq | "goodbye"

    # Test fusing
    assert (anded & anded).operand == [name_eq, age_gt, name_eq, age_gt]
    assert (anded | anded).operand != [name_eq, age_gt, name_eq, age_gt]
    assert (ored | ored).operand == [name_eq, age_gt, name_eq, age_gt]
    assert (ored & ored).operand != [name_eq, age_gt, name_eq, age_gt]


def test_build_expr():
    """Test building an expression from a query dictionary"""
    name_eq = expr.Comparison("name", expr.Eq("frank"))
    assert expr.build_expr(name_eq) is name_eq

    assert isinstance(expr.build_expr({}), expr.Empty)

    assert isinstance(expr.build_expr({"name": "tom", "age": 54}), expr.And)


def test_query_overlapping_filter_keys():
    gt_24 = expr.Comparison("age", expr.Gt(24))
    lt_38 = expr.Comparison("age", expr.Lt(38))
    compound1 = gt_24 & lt_38
    compound2 = gt_24 & lt_38
    query_filter = expr.Query(compound1, compound2).get_filter()
    assert query_filter == {
        "$and": [expr.query_expr(compound1), expr.query_expr(compound2)]
    }


def test_queryable():
    """Test queryable operators result in MongoDB expressions that we expect"""
    field_name = "test"
    value = "value"
    list_value = "val1", "val2"

    class TestQueryable(expr.Queryable):
        field = field_name

        def get_path(self) -> str:
            return self.field

    queryable = TestQueryable()

    # Check that the field name cannot be None
    with pytest.raises(ValueError):
        queryable.field = None
        queryable == value

    queryable.field = field_name

    # Special case for equals which drops the operator
    assert expr.query_expr(queryable == value) == {field_name: value}

    # Check that 'simple' operators (i.e. field <op> value)
    simple_operators = {
        "__ne__": "$ne",
        "__gt__": "$gt",
        "__ge__": "$gte",
        "__lt__": "$lt",
        "__le__": "$lte",
    }
    for attr, op in simple_operators.items():
        query_expr = expr.query_expr(getattr(queryable, attr)(value))
        assert query_expr == {field_name: {op: value}}

    # Check operators that take a list of values
    list_operators = {
        "in_": "$in",
        "nin_": "$nin",
    }
    for attr, op in list_operators.items():
        query_expr = expr.query_expr(getattr(queryable, attr)(*list_value))
        assert query_expr == {field_name: {op: list_value}}

    # Test exists
    assert expr.query_expr(queryable.exists_(True)) == {field_name: {"$exists": True}}
    with pytest.raises(ValueError):
        expr.query_expr(queryable.exists_("true"))

    # Test regex
    assert expr.query_expr(queryable.regex_(value)) == {field_name: {"$regex": value}}
    assert expr.query_expr(queryable.regex_(value, "i")) == {
        field_name: {"$regex": value, "$options": "i"}
    }
    with pytest.raises(ValueError):
        queryable.regex_(True)

    # Test starts_with
    assert expr.query_expr(queryable.starts_with_(value)) == {
        field_name: {"$regex": f"^{value}"}
    }


def test_query_expr():
    field_name = "test"
    value = "value"

    # If you pass in a dictionary it is just returned
    assert expr.query_expr({field_name: value}) == {field_name: value}

    with pytest.raises(TypeError):
        expr.query_expr([])

    class FaultyFilterLike(expr.FilterLike):
        def __query_expr__(self) -> dict:
            return "hello"

    with pytest.raises(TypeError):
        expr.query_expr(FaultyFilterLike())
