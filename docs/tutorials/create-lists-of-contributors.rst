Create lists of contributors
#############################

How to create a list of number of contributors to all workflows
***************************************************************

Because the ``Project``'s ``classifications`` property is a ``pandas.DataFrame`` object, you can treat it as such.

That means that we can group them by ``workflow_id`` by using the DataFrame's ``.groupby()`` method, loop through that, and access the nested ``rows.user_name`` property's ``.unique()`` method.

.. code-block:: python

  for workflow_id, rows in project.classifications.groupby("workflow_id"):
    print(workflow_id, len(rows.user_name.unique()))

How to create a dictionary of contributors by workflow
******************************************************

Building on the previous section, we can also create a dictionary with the ``workflow_id`` as key and a list of the workflow's contributors (or participants) by looping over the grouped classifications.

In this case, we are using the output in a dictionary comprehension, creating the dictionary as we go.

As the ``rows.user_name`` property's ``.unique()`` method does not return a list, we need to convert it to such an object here:

.. code-block:: python

  contributors = {
    workflow_id: list(rows.user_name.unique())
    for workflow_id, rows in project.classifications.groupby("workflow_id")
  }

*Note that, here, the dictionary comprehension has been separated out on multiple lines for readability.*

Saving to a JSON file
=====================

While outside the scope of the Zoonyper package, you can also easily save a dictionary like the one created above, as a JSON file, for further processing:

.. code-block:: python

  import json

  with open("./contributors.json", "w+") as f:
    json.dump(contributors, f)
