Introduction to Zoonyper's Data Structures
==========================================

DataFrames constructed from the CSV files
-----------------------------------------

:attr:`Project.classifications <zoonyper.project.Project.classifications>` is
the classifications DataFrame, and has all the functionality of a regular
:class:`pandas.DataFrame`:

.. code-block:: python

    project.classifications.head(2)

:attr:`Project.subjects <zoonyper.project.Project.subjects>` is the subjects
DataFrame, and has the same :class:`kind of functionality <pandas.DataFrame>`:

.. code-block:: python

    project.subjects.head(2)

:attr:`Project.workflows <zoonyper.project.Project.workflows>` is the
:class:`pandas.DataFrame` representing the project's workflows:

.. code-block:: python

    project.workflows.head(2)

Shortcuts to Column Summaries
-----------------------------

:attr:`Project.workflow_ids <zoonyper.project.Project.workflow_ids>` is a list
of all of the project's workflow IDs:

.. code-block:: python

    project.workflow_ids

:attr:`Project.inactive_workflow_ids <zoonyper.project.Project.inactive_workflow_ids>`
is a list of the project's inactive workflow's IDs:

.. code-block:: python

    project.inactive_workflow_ids

By using :attr:`Project.workflow_ids <zoonyper.project.Project.workflow_ids>`
and :attr:`zoonyper.project.Project.inactive_workflow_ids`, we can get the
active workflows by using:

.. code-block:: python

    set(project.workflow_ids) - set(project.inactive_workflow_ids)

:attr:`Project.subject_sets <zoonyper.project.Project.subject_sets>` is a list
all of the project's subject sets and corresponding subject IDs:

.. code-block:: python

    project.subject_sets

:attr:`Project.subject_urls <zoonyper.project.Project.subject_urls>` is a list
of all of the project's subjects and their corresponding URLs:

.. code-block:: python

    project.subject_urls
