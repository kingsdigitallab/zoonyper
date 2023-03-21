Handling Comments
#################

Comments are an important part of ``zoopyper``'s functionality. They allow access to discussions from Zooniverse's Talk functionality, where volunteers contribute comments on the crowdsourced citizen science projects where they can discuss the project's subjects.

This section will show you how to handle comments using the ``zoopyper`` package, including how to get access to all comments, how to get pre-filtered comments, and how to get comments for a specific subject.

Getting Access to All Comments
==============================

To get access to all comments in the project, you can use the ``Project.comments`` property:

.. code-block:: python

    # Get all comments for the project
    project.comments

This will return a :class:`pandas.DataFrame` containing all the comments for the project.

Getting Pre-Filtered Comments
=============================

To get a pre-filtered comments DataFrame, including only non-staff members, you can use the ``Project.get_comments()`` method with the ``include_staff=False`` setting.

If you run it before informing the ``Project`` which users count as "staff", you will get a warning. In the example below, we start by letting the ``Project`` know who is a staff member:

.. code-block:: python

    # Set the staff property on the project to a list of usernames
    project.set_staff(["miaridge", "kallewesterling"])

    # Get pre-filtered comments
    project.get_comments(include_staff=False)

This will return a :class:`pandas.DataFrame` containing only comments from non-staff members.

Getting Comments for a Specific Subject
=======================================

To get comments for a specific subject, you can use the ``Project.get_subject_comments()`` method with the subject ID as the argument:

.. code-block:: python

    # Get comments for a specific subject
    project.get_subject_comments(73334345)

This will return a :class:`pandas.DataFrame` containing all the comments for the specified subject.

Note that, by default, the ``get_subject_comments`` method will always includes comments from contributors marked as "staff" in Zoonyper. You can disable this by informing the ``Project`` instance about which usernames count as staff and then passing the parameter ``include_staff=False`` to the method:

.. code-block:: python

    # Set the staff property on the project to a list of usernames
    project.set_staff(["miaridge", "kallewesterling"])

    # Get pre-filtered comments for the particular subject
    project.get_subject_comments(73334345, include_staff=False)
