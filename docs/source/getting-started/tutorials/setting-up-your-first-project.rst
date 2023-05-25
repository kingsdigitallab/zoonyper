Setting up your first project
=============================

Downloading and Organizing Files for Your Zoonyper Project
----------------------------------------------------------

To set up your Zoonyper project, you'll need to download all the necessary
files and store them in the same folder. Here's how to do it:

#. Go to the Lab page of your Zooniverse project and navigate to the "Data
   Exports" section.

#. Download the following files:

   * Classifications (as a CSV file)

   * Subjects (as a CSV file)

   * Workflows (as a CSV file)

   * Talk comments (as a JSON file)

   * Tags (as a JSON file)

   *Note: Be sure to select the correct files and formats, and avoid
   downloading the "workflow classifications" file.*

#. Once the files are downloaded, move them to a new folder and give each file
   a specific name:

   * ``classifications.csv`` for the classifications file
   * ``subjects.csv`` for the subjects file
   * ``workflows.csv`` for the workflows file
   * ``comments.json`` for the talk comments file
   * ``tags.json`` for the tags file

   *(Note: Remember that the last two files are JSON files and not CSV files.)*

By following these steps, you'll have all the necessary files organized and
ready to use for your Zoonyper project.

.. admonition:: Unpacking comments and tags .tar files
  :class: dropdown

  The comments and tags files are downloaded as .tar files, which need to be
  unarchived on your local machine. On macOS, you can use the built-in Archive
  Utility.

  If you'd rather use the terminal, you can run the following command:

  .. code-block:: bash

    $ tar -xvzf <path>

Initializing a ``Project`` with the Downloaded Files
----------------------------------------------------

Once you've downloaded and organized the necessary files for your Zoonyper
project (in the previous step), you can initiate a new ``Project`` in Python.
Here's how to do it:

#. Open a new Python script or Jupyter notebook and import the Zoonyper
   library:

   .. code-block:: python

     from zoonyper import Project

#. Specify the directory path where you stored the downloaded files:

   .. code-block:: python

     project = Project("path/to/input-directory")

   (Replace ``"path/to/input-directory"`` with the actual path to your
   directory.)

   This creates a new ``Project`` object that will contain all the necessary
   data from the downloaded files.

That's it! You've now successfully initialized a Zoonyper project with your
downloaded files.

.. note::

  If you are interested in alternative ways to set up a project, check out the
  :ref:`loading a project` tutorial. (The method shown here is equivalent to
  :ref:`"Option 2: Specifying directory with required files" <loading-project-option-2>`)

Disambiguating subjects (Optional)
----------------------------------

To avoid ambiguous classifications and consolidate all classifications per
actual subject (rather than the subjects uploaded to Zooniverse), you can
perform a process called *disambiguation* on the downloaded subjects.
Disambiguation involves downloading each subject image and extracting a unique
identifier for each one, which Zoonyper can use to group identical subjects
together.

To disambiguate the subjects in your Zoonyper project, follow these steps:

#. Create a new folder to store the subject image files:

   .. code-block:: bash

     $ mkdir input-directory/downloads/

#. Now, download all the subject files from your project:

   .. code-block:: python

     project.download_all_subjects(sleep=(0, 1), organize_by_workflow=False, organize_by_subject_id=False)

   Note that this step will take some time as you will have to download every
   single subject processed in your project. Depending on how many subjects you
   have across all your workflows, it may take several hours.

   By setting the ``sleep=(0, 1)`` parameter, we allow the method to wait a
   random number of seconds (between 0 and 1 in the example) in-between each
   download. If you keep running into timeout errors, you can increase these
   numbers to see if it helps.

   Setting ``organize_by_workflow=False`` and ``organize_by_subject_id=False``
   will organize the downloaded files as a flat structure in the downloads
   folder.

#. Next, call the ``.disambiguate_subjects()`` method on your ``Project``
#. object and pass in the download directory as its argument:

   .. code-block:: python

     project.disambiguate_subjects()

   This method will download each subject image and extract its unique
   identifier, which will be stored in the project's metadata. Note that this
   process may take some time depending on the number of subjects in your
   project.

That's it! You've now successfully disambiguated the subjects in your Zoonyper
project.

Finishing Up
------------

Congratulations, you've successfully set up and initialized a Zoonyper project
with your downloaded files! Here are a couple of final tips to help you get
started:

* Access the project's subjects and classifications as Pandas DataFrames:

  .. code-block:: python

    project.subjects
    project.classifications

  These two DataFrames contain all the information you need to start analyzing
  and visualizing your project data.

* Check out the Zoonyper documentation and examples for more ideas on how to
  use the library. Here are a few topics to get you started:

  * Working with workflows and tasks
  * Filtering and grouping classifications
  * Creating visualizations and summary statistics
  * Exporting data in various formats
