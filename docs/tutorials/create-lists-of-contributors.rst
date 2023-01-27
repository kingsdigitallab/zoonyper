Create lists of contributors
#############################

How to create a list of number of contributors to all workflows
***************************************************************

.. code-block:: python

  for workflow_id, rows in project.classifications.groupby("workflow_id"):
    print(workflow_id, len(rows.user_name.unique()))

How to create a dictionary of contributors by workflow
******************************************************

.. code-block:: python

  contributors = {
    workflow_id: list(rows.user_name.unique())
    for workflow_id, rows in project.classifications.groupby("workflow_id")
  }

_Note that, here, the dictionary comprehension has been separated out on multiple lines for readability.

Saving to a JSON file
=====================

While outside the scope of the Zoonyper package, you can also easily save a dictionary like the one created above, as a JSON file, for further processing:

.. code-block:: python

  import json

  with open("./contributors.json", "w+") as f:
    json.dump(contributors, f)
