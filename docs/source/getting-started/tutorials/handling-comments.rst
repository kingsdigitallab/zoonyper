Handling Comments
#################

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
