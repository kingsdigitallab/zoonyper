Workflow Timelines
##################

``Project.get_workflow_timelines()`` provides the data for the extent of classifications for all workflows in a given project:

.. code-block:: python

    project.get_workflow_timelines()

``Project.get_workflow_timelines(include_active=False)`` does the same as above, but excludes active workflows from the list:

.. code-block:: python

    project.get_workflow_timelines(include_active=False)
