

.. _repository: https://github.com/muhrin/mincepy


Development
===========

The mincePy source code, issues, etc are all kept at our `repository`_.

Clone the project:

.. code-block:: shell

    git clone https://github.com/muhrin/mincepy.git
    cd mincepy


Create a new virtualenv:

.. code-block:: shell

    virtualenv -p python3 mincepy

Install all requirements:

.. code-block:: shell

    env/bin/pip install -e '.[dev,docs]'
