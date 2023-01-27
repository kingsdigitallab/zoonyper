Setting up a project
#####################

.. code-block:: python

  from zoonyper import Project

  project = Project("input-directory")

  project.disambiguate_subjects("input-directory/downloads/")

Now you can access the subjects (which is a DataFrame):

.. code-block:: python

  project.subjects

. . . and the classifications (which is also a DataFrame):

.. code-block:: python

  project.classifications
