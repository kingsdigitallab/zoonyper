Installing Zoonyper
###################

=================================
Quick Install in Jupyter Notebook
=================================

If you are in a hurry and want to install the developer version in whichever Python kernel you're currently running in a Notebook, here's a handy bash script that you can copy and paste into a Jupyter notebook:

.. code-block:: bash

    %%bash

    curl -sSL https://install.python-poetry.org | python - &&
    if [ ! -d "zoonyper" ] ; then git clone git@github.com:Living-with-machines/zoonyper.git; fi &&
    cd zoonyper &&
    poetry shell &&
    poetry build &&
    pip install dist/zoonyper-0.1.0.tar.gz


==============================
Using PyPI to install Zoonyper
==============================

When in production, you can use PyPI to install Zoonyper:

.. code-block:: bash

    $ pip install zoonyper

.. warning::

    This command will not work currently, as this package is not yet published on PyPI.

=================================
In development
=================================

Because this project is in active development, you will likely need to install from the repository for the time being.

In order to do so, you need to first ensure that you have installed Poetry:

.. code-block:: bash

    $ curl -sSL https://install.python-poetry.org | python3 -

.. warning::

    Make sure the command above, after the pipe ``|`` refers to the correctly linked Python, i.e. you may want it to refer to ``python`` or ``python3`` or whatever your symlinked binary is called.

.. note::

    Don't forget to add `export PATH="/home/<username>/.local/bin:$PATH"` to your shell configuration file in order to get access to the ``poetry`` tool on your command line.

Then, clone the repository:

.. code-block:: bash

    $ git clone git@github.com:Living-with-machines/zoonyper.git

Once it has been cloned, navigate into the newly created local repository:

.. code-block:: bash

    $ cd zoonyper

First, you will want to activate Poetry's shell:

.. code-block:: bash

    $ poetry shell

Then, install the dependencies:

.. code-block:: bash

    $ poetry install

.. note::

    You may run into an issue here, with the installation of ``furo``:

    .. code-block:: bash

        • Installing furo (2022.12.7): Failed

            _WheelFileValidationError

            ["In /home/<username>/.cache/pypoetry/artifacts/38/be/e4/0afbe5654cdc0168ebfaf6864c20009c2eec3dd953961a7d44e0ed3fe9/furo-2022.12.7-py3-none-any.whl, hash / size of furo/__init__.py didn't match RECORD", "In /home/<username>/.cache/pypoetry/artifacts/38/be/e4/0afbe5654cdc0168ebfaf6864c20009c2eec3dd953961a7d44e0ed3fe9/furo-2022.12.7-py3-none-any.whl, hash / size of furo/_demo_module.py didn't match RECORD", ... [etc]

    If this is the case, see the solution here: https://github.com/python-poetry/poetry/issues/7691#issue-1632193622

    The easiest solution is to exit poetry (by running ``exit`` and running a ``pip install poetry==1.4.0``).

    This is a problem with poetry 1.4.1 so it may be solved by the time you're reading this.

Following that, you can run a build and ``pip install`` from the local files:

.. code-block:: bash

    $ poetry build && pip install dist/zoonyper-0.1.0.tar.gz

Now you should be able to use ``zoonyper`` as a regular package on your local computer.

.. warning::

    If you change the source code (located in the ``zoonyper`` directory in the repository), you will need to rerun the ``poetry build`` and the ``pip install dist/zoonyper-0.1.0.tar.gz`` commands again.
