Project and Workflow Participant Lists
######################################

``Project.participants()`` offers a quick way of seeing *all participants across the entire project*:

.. code-block:: python
    
    project.participants()

``Project.participants(by_workflow=True)`` can be used to see all participants by workflow (as a dictionary):

.. code-block:: python

    project.participants(by_workflow=True)

``Project.participants(workflow_id=<id>)`` can finally be used to retrieve participants for a particular workflow:

.. code-block:: python

    project.participants(workflow_id=12038)

==============================================
Counting participants
==============================================

Counting participants across projects and workflows is also a quickly accessible functionality of the package.

``Project.participants_count()`` offers a quick way to see how many participants were in the project altogether:

.. code-block:: python

    project.participants_count()

If the function is provided with an optional workflow ID, ``Project.participants_count(<workflow_id>)`` can also be used to see how many participants were in a particular workflow:

.. code-block:: python

    project.participants_count(12194)

==============================================
Logged in participants
==============================================

``Project.logged_in()`` can be used to counting how many classifications were done while users were logged in:

.. code-block:: python

    project.logged_in()

Similar to the functionality above, if the same function is provided with an optional workflow ID, ``Project.logged_in(<workflow_id>)``, we can also see how many classifications were made while logged in for a particular workflow ID:

.. code-block:: python
    
    project.logged_in(12194)
