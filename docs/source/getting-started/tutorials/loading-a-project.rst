Loading a Project
#################

Introduction
============

When working with Zoonyper ``Project`` objects in Python, one of the first steps is to load the required files into a Project object. This section will show you how to load a project using the ``Project`` class in the ``zoopyper`` package.

Required Files
==============

To load a project, you will need to have the following files available:

* classifications.csv
* subjects.csv
* workflows.csv
* comments.json
* tags.json

These files contain the data needed to define the project and its associated tasks.

You can learn more about how to set those files up in :ref:`setting up your first project`.

Loading a Project's Files
=========================

There are two ways to load a project's files into ``zoonyper``: by specifying individual file paths or by specifying a directory with the required files.

Option 1: Specifying Individual File Paths
------------------------------------------

Providing the path for each of the five required files has the benefit of you being able to specify exactly where the files are located, individually:

.. code-block:: python
    :name: loading-project-option-1
    :linenos:

    # Import the Project class
    from zoopyper import Project

    # Specify the file paths
    classifications_path = "<full-path-to>/classifications.csv"
    subjects_path = "<full-path-to>/subjects.csv"
    workflows_path = "<full-path-to>/workflows.csv"
    comments_path = "<full-path-to>/comments.json"
    tags_path = "<full-path-to>/tags.json"

    # Create the Project object
    project = Project(
            classifications_path=classifications_path,
            subjects_path=subjects_path,
            workflows_path=workflows_path,
            comments_path=comments_path,
            tags_path=tags_path
        )

Here's what each line of code does:

* ``from zoopyper import Project``: Imports the ``Project`` class from the ``zoopyper`` package.
* ``classifications_path = "<full-path-to>/classifications.csv"``: Specifies the full path to the ``classifications.csv`` file.
* ``subjects_path = "<full-path-to>/subjects.csv"``: Specifies the full path to the ``subjects.csv`` file.
* ``workflows_path = "<full-path-to>/workflows.csv"``: Specifies the full path to the ``workflows.csv`` file.
* ``comments_path = "<full-path-to>/comments.json"``: Specifies the full path to the ``comments.json`` file.
* ``tags_path = "<full-path-to>/tags.json"``: Specifies the full path to the ``tags.json`` file.
* ``project = Project(...)``: Creates a ``Project`` object using the specified file paths.

Option 2: Specifying directory with required files
--------------------------------------------------

In the example above, if all the required files (``classifications.csv``, ``subjects.csv``, ``workflows.csv``, ``comments.json`` and ``tags.json``) are located in the same path, you can just provide the path where all of them are located:

.. code-block:: python
    :name: loading-project-option-2

    # Import the Project class
    from zoopyper import Project

    # Specify the path to the directory with all the required files
    path = "<full-path-to-directory>"

    # Create the Project object
    project = Project(path=path)

Here's what each line of code does:

* ``from zoopyper import Project``: Imports the ``Project`` class from the ``zoopyper`` package.
* ``path = "<full-path-to-directory>"``: Specifies the full path to the directory containing all the required files.
* ``project = Project(path=path)``: Creates a ``Project`` object using the specified path to the directory containing all the required files.

Tips
----

* Verify that the required files are in the correct format before loading them into the Project object.
* Use a consistent naming convention for the file paths to make it easier to manage and maintain your code.
* Ensure that the file paths are correct, as incorrect paths can lead to errors and prevent the project from loading correctly.
* If you are working with a large project, consider breaking up the data into smaller, more manageable files to make it easier to work with.

By following these tips, you can ensure that your project is loaded correctly and that you can begin working with the data right away.
