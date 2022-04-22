Data Structures
###############


``Project.classifications`` is the classifications DataFrame, and has all the functionality of a regular Pandas DataFrame:

.. code-block:: python

    project.classifications.head(2)

``Project.subjects`` is the subjects DataFrame, and has the same kind of functionality:

.. code-block:: python
    
    project.subjects.head(2)

``Project.workflows`` is the DataFrame variation for the project's workflows:

.. code-block:: python
    
    project.workflows.head(2)

==============================================
Shortcuts to Column Summaries
==============================================

``Project.workflow_ids`` (an attribute) lists all of the project's workflow IDs:

.. code-block:: python
    
    project.workflow_ids

``Project.inactive_workflow_ids`` (an attribute) lists the project's inactive workflow's IDs:

.. code-block:: python
    
    project.inactive_workflow_ids

Using ``Project.workflow_ids`` and ``Project.inactive_workflow_ids``, we can get the active workflows by using:

.. code-block:: python

    set(project.workflow_ids) - set(project.inactive_workflow_ids)

``Project.subject_sets`` (an attribute) lists all of the project's subject sets and corresponding subject IDs:

.. code-block:: python

    project.subject_sets

``Project.subject_urls`` lists all of the project's subjects and their corresponding URLs:

.. code-block:: python

    project.subject_urls
