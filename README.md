# About ``zoonyper``

[![All Contributors](https://img.shields.io/github/all-contributors/Living-with-machines/zoonyper?color=ee8449&style=flat-square)](#contributors)

**Zoonyper** is a Python library, designed to make it easy for users to import and process Zooniverse annotations and their metadata in your own Python code. It is especially designed for use in [Jupyter Notebooks](https://jupyter.org/).

## Background

The library was created as part of the Living with Machines project, a project aimed to generate new historical perspectives on the effects of the mechanisation of labour on the lives of ordinary people during the long nineteenth century. As part of that work, we have used newspapers for historical research at scale. In that work, it has been important for us to use the newspapers also as source documents for crowdsourced activities. The platform used for the crowdsourced activities is Zooniverse, created for citizen science projects where volunteers contribute to scientific research projects by annotating and categorizing images or other data. The annotations created by volunteers are collected as "classifications" in the Zooniverse system.

In the Living with Machines project, we used the Zooniverse platform to annotate articles extracted from historical newspapers. We winnowed out articles that were deemed unsuitable or irrelevant for the study, and then asked volunteers to help us with more detailed classifications on the remaining articles. This helps to ensure that the annotations are focused and accurate, and that the results of the study are meaningful and relevant. The articles, along with metadata, were included in Zooniverse manifests. The final goal for the research overall was to use the annotations to study the content of these historical newspapers and gain insights into the events and trends of the past.

## Getting started

Here's how you can use Zoonyper in your own project:

1. **Install the repository**: First, you'll need to install the repository. You can do this by cloning the repository or installing it using [the instructions below](#installation).

2. **Import the `Project` class**: Once you've installed the repository, you can import the `Project` class into your own Python code. You can do this by adding the following line to the top of your code:

    ```py
    from zoonyper import Project
    ```

3. **Initialize a `Project` object**: To start using the Project class, you'll need to create a Project object. You can do this by calling the constructor and passing in the path to the Zooniverse annotations file:

    ```py
    project = Project("<path to the directory with all necessary files>")
    ```

4. **Access the project's data and metadata**: Once you have a `Project` object, you can access its annotations by using the `.classifications` attribute. This attribute is a Pandas DataFrame, where each row contains information about a single classification, including annotations.

5. **Process the data and metadata**: Because the data structures in Zoonyper are Pandas DataFrames, you can process the classifications, subjects, and annotations in any way you like, using the tools and techniques that you're familiar with. For example, you might want to calculate statistics about the annotations, or create plots to visualize the data.

## Installation

<!--
Installing through `pip`:

```sh
$ pip install zoonyper
```
-->

Because this project is in **active development**, you need to install from the repository for the time being. In order to do so, follow [the installation instructions](docs/source/getting-started/install.rst).

## Documentation

The full documentation is currently available with [`sphinx`](https://www.sphinx-doc.org/en/master/) in the [`docs`](docs) directory.

## Contributors

<!-- ALL-CONTRIBUTORS-LIST:START -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
