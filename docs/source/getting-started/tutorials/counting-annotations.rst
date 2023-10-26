Counting annotations
====================

:meth:`zoonyper.project.Project.classification_counts` is a useful method for
retrieving the number of different classifications per subject ID for any given
workflow. It takes two arguments, the workflow ID (passed as ``workflow_id``)
and the task number (``task_number``) that you want to extract:

.. code-block:: python

    project.classification_counts(workflow_id=12038, task_number=0)

.. note::

  The method currently works best with text annotations.

Using ``classification_counts``, we can also easily check for "agreement", say
when all annotators have agreed on *one* classification:

.. code-block:: python
    :linenos:
    :emphasize-lines: 3,4,5,6

    classifications = project.classification_counts(workflow_id=12038, task_number=0)

    agreement = {
        subject_id: len(unique_classifications) == 1
        for subject_id, unique_classifications in classifications.items()
    }

    print(agreement)

Similarly, we can construct a code block for whenever at least **four
annotators** have agreed on one response for a subject:

.. code-block:: python
    :linenos:
    :emphasize-lines: 3,4,5,6

    classifications = project.classification_counts(workflow_id=12038, task_number=0)

    agreement = {
        subject_id: len([classification for classification, count in unique_classifications.items() if count > 4]) == 1
        for subject_id, unique_classifications in classifications.items()
    }

    print(agreement)
