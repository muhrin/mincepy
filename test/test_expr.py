import pytest

from mincepy import expr

# pylint: disable=pointless-statement, invalid-name


def test_expr_types_and_filters():
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

    with pytest.raises(TypeError):
        # Comparison takes an operator, not a string
        expr.Comparison('my_field', 'oper')


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

    # Test fusing
    assert (anded & anded).operand == [name_eq, age_gt, name_eq, age_gt]
    assert (anded | anded).operand != [name_eq, age_gt, name_eq, age_gt]
    assert (ored | ored).operand == [name_eq, age_gt, name_eq, age_gt]
    assert (ored & ored).operand != [name_eq, age_gt, name_eq, age_gt]


def test_build_expr():
    """Test building an expression from a query dictionary"""
    name_eq = expr.Comparison('name', expr.Eq('frank'))
    assert expr.build_expr(name_eq) is name_eq

    assert isinstance(expr.build_expr({}), expr.Empty)

    assert isinstance(expr.build_expr({'name': 'tom', 'age': 54}), expr.And)


def test_query_overlapping_filter_keys():
    gt_24 = expr.Comparison('age', expr.Gt(24))
    lt_38 = expr.Comparison('age', expr.Lt(38))
    compound1 = gt_24 & lt_38
    compound2 = gt_24 & lt_38
    query_filter = expr.Query(compound1, compound2).get_filter()
    assert query_filter == {'$and': [expr.query_expr(compound1), expr.query_expr(compound2)]}
