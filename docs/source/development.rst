

.. _repository: https://github.com/muhrin/mincepy


Development
===========

The mincePy source code, issues, etc are all kept at our `repository`_.

Clone the project:

.. code-block:: shell

    git clone https://github.com/muhrin/mincepy.git
    cd mincepy


Create a new virtual environment (Python 3.11 or later):

.. code-block:: shell

    python3 -m venv env

Install all requirements and the pre-commit hooks:

.. code-block:: shell

    env/bin/pip install -e '.[dev,docs]'
    env/bin/pre-commit install

Code is linted and formatted with `ruff`_, which runs as part of the pre-commit hooks.  You can
also run all the checks by hand:

.. code-block:: shell

    env/bin/pre-commit run --all-files


Running the tests
-----------------

By default the tests connect to a MongoDB server at ``mongodb://localhost``.  To run them without a
server, point them at an in-memory `litemongo`_ database instead:

.. code-block:: shell

    MINCEPY_TEST_URI="litemongo://?engine=memory#mincepy-tests" env/bin/pytest --benchmark-skip test

.. _ruff: https://docs.astral.sh/ruff/
.. _litemongo: https://github.com/muhrin/litemongo
