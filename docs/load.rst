Loading the Base Class
#############################################################

Loading a project is done using the ``Project`` class:

.. py:class:: Project(path='', classifications_path='', subjects_path='', \
    workflows_path='', comments_path='', tags_path='')

You have two options to load a project:

#. Provide paths to each individual file (using all the specific path arguments).
#. Provide a general path (using the ``path`` keyword argument, **preferred**).

=================================
Option 1: Individual file paths
=================================

Providing the path for each of the five required files has the benefit of you being able to specify exactly where the files are located, individually:

.. code-block:: python

    classifications_path = "<full-path-to>/classifications.csv"
    subjects_path = "<full-path-to>/subjects.csv"
    workflows_path = "<full-path-to>/workflows.csv"
    comments_path = "<full-path-to>/comments.json"
    tags_path = "<full-path-to>/tags.json"

    project = Project(
            classifications_path=classifications_path,
            subjects_path=subjects_path,
            workflows_path=workflows_path,
            comments_path=comments_path,
            tags_path=tags_path
        )

=================================
Option 2: Directory
=================================

In the example above, if all the required files (``classifications.csv``, ``subjects.csv``, ``workflows.csv``, ``talk-comments.json`` and ``talk-tags.json``) are located in the same path, you can just providing the path where all of them are located, which is a neater way of writing the same thing:

.. code-block:: python

    project = Project("<full-path-to-all-files>")
