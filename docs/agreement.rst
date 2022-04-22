Counting annotations
####################

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
