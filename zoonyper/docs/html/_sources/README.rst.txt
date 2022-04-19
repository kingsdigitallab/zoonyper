#############################################################
About ``zoonyper``
#############################################################

Zoonyper facilitates interpretation and wrangling for Zooniverse files in Jupyter (and Python).

#############################################################
Importing a Project
#############################################################

You import the `Project` class from the `zoonyper` library like this:

.. code-block:: python

    from zoonyper import Project

#############################################################
Loading a Project
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

#############################################################
Access All Project Data (Frames)
#############################################################


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

#############################################################
Listing Project and Workflow Participants
#############################################################

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

#############################################################
Counting classifications
#############################################################

``Project.classification_counts(workflow_id=<workflow ID>, task_number=<task number>)`` is a method that retrieves the number of different classifications per subject ID for any given workflow:

.. code-block:: python

    project.classification_counts(workflow_id=12038, task_number=0)

*Note: The method currently works best with text annotations.*

Using ``classification_counts``, we can also easily check for "agreement", say when all annotators have agreed on *one* classification:

.. code-block:: python

    agreement = {
        subject_id: len(unique_classifications) == 1
        for subject_id, unique_classifications in project.classification_counts(workflow_id=12038, task_number=0).items()
    }

    print(agreement)

Similarly, we can construct a code block for whenever at least **four annotators** have agreed on one response for a subject:

.. code-block:: python
    
    agreement = {
        subject_id: len([classification for classification, count in unique_classifications.items() if count > 4]) == 1
        for subject_id, unique_classifications in project.classification_counts(workflow_id=12038, task_number=0).items()
    }

    print(agreement)

#############################################################
Workflow Timelines
#############################################################

``Project.get_workflow_timelines()`` provides the data for the extent of classifications for all workflows in a given project:

.. code-block:: python

    project.get_workflow_timelines()

``Project.get_workflow_timelines(include_active=False)`` does the same as above, but excludes active workflows from the list:

.. code-block:: python

    project.get_workflow_timelines(include_active=False)

#############################################################
Comments
#############################################################

``Project.comments`` (a property) provides access to all the comments in the project as a pandas DataFrame.

.. code-block:: python
    
    project.comments

To get a pre-filtered comments DataFrame, including only non-staff members, you have to first set the staff property on the Project and then use the `Project.get_comments(include_staff=False)` method instead, using the ``include_staff`` setting set to ``False``:

.. code-block:: python
    
    project.set_staff(["miaridge", "kallewesterling"])
    project.get_comments(include_staff=False)

``Project.get_subject_comments(<subject_id>)`` offers a quick-access method on the ``Project`` to see the comments for each subject as a DataFrame (it always includes staff comments):

.. code-block:: python
    
    project.get_subject_comments(73334345)

.. toctree::
   :maxdepth: 3
   :caption: Contents:

   README
