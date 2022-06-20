Installing Zoonyper
#############################################################

When in production, you can use PyPI to install zoonyper:

.. code-block:: bash

    $ pip install zoonyper

=================================
In development
=================================

Because this project is in active development, you will likely need to install from the repository for the time being.

In order to do so, you need to first ensure that you have installed Poetry:

.. code-block:: bash

    $ curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

Then, clone the repository:

.. code-block:: bash

    $ git clone git@github.com:Living-with-machines/zoonyper.git

Once it has been cloned, navigate into the newly created local repository:

.. code-block:: bash

    $ cd zoonyper

Following that, you can run a build and ``pip install`` from the local files:

.. code-block:: bash

    $ poetry build && pip install dist/zoonyper-0.1.0.tar.gz

Now you should be able to use ``zoonyper`` as a regular package on your local computer.

.. warning::

    If you change the source code (located in the ``zoonyper`` directory in the repository), you will need to rerun the ``poetry build`` and the ``pip install dis/zoonyper-0.1.0.tar.gz`` commands again.

=================================
Quick Install in Jupyter Notebook
=================================

If you are in a hurry and want to install the developer version in whichever Python kernel you're currently running in a Notebook, here's a handy bash script that you can copy and paste into a Jupyter notebook:

.. code-block:: bash

    %%bash
    
    curl -sSL https://install.python-poetry.org | python - &&
    if [ ! -d "zoonyper" ] ; then git clone git@github.com:Living-with-machines/zoonyper.git; fi &&
    cd zoonyper &&
    poetry build &&
    pip install dist/zoonyper-0.1.0.tar.gz
