==================
About ``zoonyper``
==================

**Zoonyper** is a Python library, designed to make it easy for users to import
and process Zooniverse annotations and their metadata in your own Python code.
It is especially designed for use in [Jupyter Notebooks](https://jupyter.org/).

Purpose
-------

Zoonyper can process the output files from the Zooniverse citizen science
platform, and facilitate data wrangling, compression, and output into JSON and
CSV files. The output files can then be more easily used in e.g. Observable
visualisations.

Background
----------

The library was created as part of the
`Living with Machines project <https://livingwithmachines.ac.uk>`_, a project
aimed to generate new historical perspectives on the effects of the
mechanisation of labour on the lives of ordinary people during the long
nineteenth century. As part of that work, we have used newspapers for
historical research at scale. In that work, it has been important for us to
use the newspapers also as source documents for crowdsourced activities. The
platform used for the crowdsourced activities is Zooniverse, created for
citizen science projects where volunteers contribute to scientific research
projects by annotating and categorizing images or other data. The annotations
created by volunteers are collected as "classifications" in the Zooniverse
system.

In the Living with Machines project, we used the Zooniverse platform to
annotate articles extracted from historical newspapers. We winnowed out
articles that were deemed unsuitable or irrelevant for the study, and then
asked volunteers to help us with more detailed classifications on the remaining
articles. This helps to ensure that the annotations are focused and accurate,
and that the results of the study are meaningful and relevant. The articles,
along with metadata, were included in Zooniverse manifests. The final goal for
the research overall was to use the annotations to study the content of these
historical newspapers and gain insights into the events and trends of the past.

.. note::

   This project is under active development.

Contents
========

.. toctree::
   :maxdepth: 3

   installing
   getting-started/index
   reference

..
   import
   load
   data-structure
   downloading-subject-files
   participants
   agreement
   timelines
   comments
   disambiguation-deduplication


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
