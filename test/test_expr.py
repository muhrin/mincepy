import pytest

from mincepy import expr

# pylint: disable=pointless-statement


def test_query_filters_dicts():
    """Test the query filters for expressions"""
    name_eq = expr.Comparison('name', expr.Eq('frank'))
    age_gt = expr.Comparison('age', expr.Gt(38))

    # Check all operators
    for expression in expr.Operator.__subclasses__():
        assert expression.oper.startswith('$')
        assert expression(123).dict() == {expression.oper: 123}

    # Check all logicals
    for list_expr in expr.Logical.__subclasses__():
        assert list_expr.oper.startswith('$')
        if issubclass(list_expr, expr.WithListOperand):
            assert list_expr([name_eq, age_gt]).dict() == \
                   {list_expr.oper: [name_eq.dict(), age_gt.dict()]}

            # Check that the the passthrough for list expressions work
            assert list_expr([name_eq]).dict() == name_eq.dict()

            with pytest.raises(TypeError):
                list_expr('non list')
            with pytest.raises(TypeError):
                list_expr(['non expression'])
        else:
            assert list_expr(name_eq).dict() == {list_expr.oper: name_eq.dict()}
            with pytest.raises(TypeError):
                list_expr(['non expression'])

    assert expr.Empty().dict() == {}


def test_expr_and_or():
    """Test expression and/or methods"""
    name_eq = expr.Comparison('name', expr.Eq('frank'))
    age_gt = expr.Comparison('age', expr.Gt(38))

    anded = name_eq & age_gt
    assert anded.dict() == {'$and': [name_eq.dict(), age_gt.dict()]}

    ored = name_eq | age_gt
    assert ored.dict() == {'$or': [name_eq.dict(), age_gt.dict()]}

    with pytest.raises(TypeError):
        name_eq & 'hello'

    with pytest.raises(TypeError):
        name_eq | 'goodbye'


def test_build_expr():
    """Test building an expression from a query dictionary"""
    name_eq = expr.Comparison('name', expr.Eq('frank'))
    assert expr.build_expr(name_eq) is name_eq

    assert isinstance(expr.build_expr({}), expr.Empty)

    assert isinstance(expr.build_expr({'name': 'tom', 'age': 54}), expr.And)
