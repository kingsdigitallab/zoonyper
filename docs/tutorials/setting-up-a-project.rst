Setting up a project
#####################

Setting up a Zoonyper ``Project`` requires you to have all the necessary files downloaded and in the same folder.

From the Lab page on your Zooniverse project, you'll want to visit the "Data Exports" section, and export all the classifications, the subjects, the workflows (N.B. *not* the "workflow classifications"), the talk comments and tags documents.

Once they're downloaded, put them all into the same folder and name the classifications file, ``classifications.csv``, the subjects file ``subjects.csv``, the workflows file ``workflows.csv``, the comments file ``comments.json`` and the tags file ``tags.json``. *Note that the last two files are JSON files and not CSV files.*

After that, you can initiate a new ``Project`` by passing the directory's path to the initiator of the class, like this:

.. code-block:: python

  from zoonyper import Project

  project = Project("input-directory")

Next, what you'll likely want to do (which will also take some time) is to *disambiguate* the subjects. Disambiguation is the process of downloading each subject as an image and subsequently extracting the hex digest for each of them. This makes it possible for us to compare which files are identical across the subjects in the project, to avoid any unintentional ambiguous classifications, and consolidating all classifications per *actual subject* rather than the subjects uploaded to Zooniverse (which can overlap).

The ``Project`` class comes with a method for this, ``.disambiguate_subjects()`` which takes a download directory as its only argument:

.. code-block:: python

  project.disambiguate_subjects("input-directory/downloads/")

Now you can access the subjects of the project as a Pandas DataFrame:

.. code-block:: python

  project.subjects

. . . and the classifications of the project, which is also provided as a Pandas DataFrame:

.. code-block:: python

  project.classifications
