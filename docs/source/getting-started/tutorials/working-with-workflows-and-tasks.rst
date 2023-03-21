Working with Workflows and Tasks
################################

Zoonyper projects can include one or more workflows, each of which consists of a series of tasks that volunteers complete to classify the project's subjects. Here's how you can work with workflows and tasks in Zoonyper:

#. Access the project's workflows as a pandas DataFrame:

   .. code-block:: python

    project.workflows

   This returns a DataFrame with the workflow ID as the index column and the other column keeping information like:

   * Display name
   * Version
   * Classification counts

   See the API documentation for more detailed information about the contents of the workflows DataFrame.

#. Get subject IDs from a specific workflow's:

   .. code-block:: python

    workflow_id = project.workflow_ids[0]
    project.workflow_subjects(workflow_id=workflow_id)

   This returns a list of the subjects for the specified workflow ID. In this example, we pick the first of the workflow IDs from the list of all the workflow IDs, which can be accessed using the ``Project``'s ``workflow_ids`` property.



