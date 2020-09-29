from mincepy import expr


def test_query_filters_dicts():
    """Test the query filters for expressions"""
    name_eq = expr.Match('name', expr.Eq('frank'))
    age_gt = expr.Match('age', expr.Gt(38))

    for expression in (expr.Eq, expr.Gt, expr.Gte, expr.Lt, expr.Lte, expr.Ne):
        assert expression.oper.startswith('$')
        assert expression(123).dict() == {expression.oper: 123}

    for list_expr in (expr.And, expr.Or, expr.Nor):
        assert list_expr.oper.startswith('$')
        assert list_expr([name_eq, age_gt]).dict() ==\
               {list_expr.oper: [name_eq.dict(), age_gt.dict()]}


def test_expr_and_or():
    """Test expression and/or methods"""
    name_eq = expr.Match('name', expr.Eq('frank'))
    age_gt = expr.Match('age', expr.Gt(38))

    anded = name_eq & age_gt
    assert anded.dict() == {'$and': [name_eq.dict(), age_gt.dict()]}

    ored = name_eq | age_gt
    assert ored.dict() == {'$or': [name_eq.dict(), age_gt.dict()]}
