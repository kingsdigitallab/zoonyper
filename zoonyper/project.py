from collections import ChainMap, Counter
from matplotlib.figure import Figure
from pathlib import Path
from tqdm import tqdm
from typing import Dict, List, Literal, Optional, Tuple, Union

import pandas as pd

import hashlib
import json
import os
import random
import requests
import time

from .utils import Utils, TASK_COLUMN, get_current_dir
from .log import log


"""
TODO: this is not elegant but here we are - to save `flattened[column]`
assignment below
"""
pd.options.mode.chained_assignment = None  # type:ignore


class Project(Utils):
    """
        A Zoonyper project represents a single Zooniverse project and contains
    all of the associated data required for analysis and visualization.

    Parameters
    ----------
    path : str, optional
        A directory that contains all five required files for the project:
        ``classifications.csv``, ``subjects.csv``, ``workflows.csv``,
        ``comments.json``, and ``tags.json``. If a path is not provided,
        the individual file paths can be passed instead.
    classifications_path : str, optional
        The path to the project's classifications CSV file.
    subjects_path : str, optional
        The path to the project's subjects CSV file.
    workflows_path : str, optional
        The path to the project's workflows CSV file.
    comments_path : str, optional
        The path to the project's comments JSON file.
    tags_path : str, optional
        The path to the project's tags JSON file.
    redact_users : bool, optional
        Whether to redact user names in the classifications table.
        Defaults to True.
    trim_paths : bool, optional
        Whether to trim file paths in columns known to contain them.
        Defaults to True.
    parse_dates : str, optional
        If specified, a list of column names to be parsed as datetime objects
        when reading the CSV files. The default value is "%Y-%m-%d", which
        will parse columns named "created_at" and "updated_at".

    Raises
    ------
    RuntimeError
        If either the paths for each of the five files are not provided or a
        general path is provided but does not contain all five files.

    Notes
    -----
    The Project class provides a high-level interface for working with
    Zooniverse projects. Use the attributes to access the project's data
    as pandas DataFrames, and use the methods to manipulate the data and
    perform analysis.
    """

    staff = []
    _workflow_timeline = []
    _participants = {}
    _workflow_ids = []
    _subject_sets = {}
    _subject_urls = {}

    _raw_frames = {}

    _workflows = None
    _subjects = None
    _classifications = None
    _comments = None
    _tags = None
    _discussions = None
    _boards = None
    _flattened = None

    download_dir = "downloads"
    SUPPRESS_WARN = False

    def __init__(
        self,
        path: str = "",
        classifications_path: str = "",
        subjects_path: str = "",
        workflows_path: str = "",
        comments_path: str = "",
        tags_path: str = "",
        redact_users: bool = True,
        trim_paths: bool = True,
        parse_dates: str = "%Y-%m-%d",
    ):
        """
        Constructor method.
        """

        # Ensure that correct paths are set up
        if (
            not all(
                [
                    isinstance(classifications_path, str),
                    isinstance(subjects_path, str),
                    isinstance(workflows_path, str),
                    isinstance(comments_path, str),
                    isinstance(tags_path, str),
                ]
            )
            and not path
        ):
            raise RuntimeError(
                "Either paths for each file or a general path argument which \
                contains all necessary files must be provided."
            )

        # Test that all files are present and exist
        if path and not any(
            [
                classifications_path,
                subjects_path,
                workflows_path,
                comments_path,
                tags_path,
            ]
        ):
            path = path.rstrip("/")
            classifications_path = path + "/classifications.csv"
            subjects_path = path + "/subjects.csv"
            workflows_path = path + "/workflows.csv"
            comments_path = path + "/comments.json"
            tags_path = path + "/tags.json"

            if not all(
                [
                    Path(classifications_path).exists(),
                    Path(subjects_path).exists(),
                    Path(workflows_path).exists(),
                    Path(comments_path).exists(),
                    Path(tags_path).exists(),
                ]
            ):
                raise RuntimeError(
                    "If a general path is provided, it must contain five \
                    files: classifications.csv, subjects.csv, workflows.csv, \
                    comments.json, and tags.json"
                )

        self.classifications_path = Path(classifications_path)
        self.subjects_path = Path(subjects_path)
        self.workflows_path = Path(workflows_path)
        self.comments_path = Path(comments_path)
        self.tags_path = Path(tags_path)

        self.redact_users = redact_users
        self.trim_paths = trim_paths
        self.parse_dates = parse_dates

    @staticmethod
    def _user_logged_in(row: pd.Series) -> bool:
        """
        Determine whether a classification was made by a logged-in user.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        row : pandas.Series
            A row from the classifications DataFrame.

        Returns
        -------
        bool
            True if the user was logged in, False if not.

        Notes
        -----
        This method is used internally by the ``Project`` class to determine
        whether a classification was made by a user who was logged in to the
        Zooniverse platform at the time of classification.
        """
        return "not-logged-in" not in row if not pd.isna(row) else False

    @staticmethod
    def _extract_annotation_values(annotation_row: pd.Series) -> dict:
        """
        Extract the annotation values from a classification's annotations.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        annotation_row : pandas.Series
            A row from the classifications DataFrame containing a list of tasks
            with values in a dictionary ``{task, task_label, value}``.

        Returns
        -------
        dict
            A dictionary where the keys are task IDs and the values are the
            corresponding annotation values.

        Notes
        -----
        This method is used internally by the ``Project`` class to extract the
        annotation values from a classification's annotations, and returns them
        as a dictionary for easy insertion into a new DataFrame.
        """

        extracted_dictionaries = [
            {task_data.get("task"): task_data.get("value")}
            for task_data in annotation_row
        ]

        return dict(ChainMap(*extracted_dictionaries))

    def participants_count(
        self, workflow_id: Optional[int] = None
    ) -> Union[Dict, int]:
        """
        Get a count of the number of participants who have made classifications
        in a project's workflows.

        If a ``workflow_id`` is passed, this method returns the count of unique
        participants who made classifications in that workflow.

        If no ``workflow_id`` is passed, this method returns a dictionary where
        each key is a workflow ID and the corresponding value is the count of
        unique participants who made classifications in that workflow.

        To count the number of participants who were logged in at the time of
        their classification in a given workflow or across the project, use the
        `logged_in` method.

        .. seealso::

            :meth:`zoonyper.project.Project.logged_in` for the
            count of participants who were logged in at the time of their
            classification in a given workflow or across the project.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        workflow_id : int, optional
            The ID of a specific workflow for which to get the participant
            count. If not specified, the participant count for each workflow
            is returned.

        Raises
        ------
        RuntimeError
            If the specified ``workflow_id`` is not recorded in the project.

        Returns
        -------
        Union[Dict, int]
            If a ``workflow_id`` was passed, this method returns an integer
            value describing the participant count for that workflow. If no
            ``workflow_id`` was passed, a dictionary with workflow IDs as keys
            and their participant counts as values is returned.
        """

        if workflow_id:
            results = [
                len({x for x in rows.user_name})
                for _workflow_id, rows in self.classifications.groupby(
                    "workflow_id"
                )
                if _workflow_id == workflow_id
            ]
            if len(results) == 1:
                return results[0]
            else:
                raise RuntimeError(
                    f"No participants recorded for workflow {workflow_id}"
                )

        result = {
            workflow_id: len({x for x in rows.user_name})
            for workflow_id, rows in self.classifications.groupby(
                "workflow_id"
            )
        }

        result["total"] = len({x for x in self.classifications.user_name})

        return result

    def logged_in(self, workflow_id: Optional[int] = None) -> Union[Dict, int]:
        """
        Get a count of the number of participants who were logged in to the
        Zooniverse platform at the time of their classification in a project's
        workflows.

        If a ``workflow_id`` is passed, this method returns the count of unique
        participants who were logged in at the time of classification in that
        workflow.

        If no ``workflow_id`` is passed, this method returns a dictionary where
        each key is a workflow ID and the corresponding value is the count of
        unique participants who were logged in at the time of classification in
        that workflow.

        To count the total number of participants who made classifications in
        a given workflow or across the project, use the ``participants_count``
        method.

        .. seealso::

            :meth:`zoonyper.project.Project.participants_count` for the
            general count of participants in a given workflow or across the
            project.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        workflow_id : int, optional
            The ID of a specific workflow for which to get the logged-in
            participant count. If not specified, the logged-in participant
            count for each workflow is returned.

        Raises
        ------
        RuntimeError
            If the specified ``workflow_id`` is not recorded in the project.

        Returns
        -------
        Union[Dict, int]
            If a ``workflow_id`` was passed, this method returns an integer
            value
            describing the logged-in participant count for that workflow. If no
            ``workflow_id`` was passed, a dictionary with workflow IDs as keys
            and their corresponding logged-in participant counts as values is
            returned.
        """

        if workflow_id:
            results = [
                len([x for x in rows.user_logged_in if x])
                for _workflow_id, rows in self.classifications.groupby(
                    "workflow_id"
                )
                if _workflow_id == workflow_id
            ]
            if len(results) == 1:
                return results[0]
            else:
                raise RuntimeError(
                    f"No participants recorded for workflow {workflow_id}"
                )

        result = {
            workflow_id: len([x for x in rows.user_logged_in if x])
            for workflow_id, rows in self.classifications.groupby(
                "workflow_id"
            )
        }

        result["total"] = len({x for x in self.classifications.user_logged_in})

        return result

    def classification_counts(
        self, workflow_id: int, task_number: int = 0
    ) -> Dict[int, Dict[str, int]]:
        """
        Provides the classification count by label and subject for a
        particular workflow.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        workflow_id : int
            The workflow ID for which you want to extract classifications.
        task_number : int, optional
            The task number that you want to extract from across the workflow,
            by default ``0``.

        Raises
        ------
        KeyError
            If the provided task number does not appear in any classification
            across the project.

        Returns
        -------
        dict[int, dict[str, int]]
            A dictionary with the subject ID as the key and a nested
            dictionary as the value, which in turn has the task label as the
            key and the classification count for that task label as the value.
        """
        if f"T{task_number}" not in self.classifications.columns:
            raise KeyError(
                f"Task number {task_number} does not appear in the \
                classifications."
            )

        results = self.classifications.query(f"workflow_id=={workflow_id}")

        resulting_classifications = {}
        """
        TODO: #3 [testing] What happens below if subject_ids contains multiple
            IDs? A solution could be to make subject_ids into str(subject_ids)
        """
        for subject_ids, rows in results.groupby("subject_ids"):
            try:
                all_results = [str(x) for x in rows[f"T{task_number}"]]
            except KeyError:
                raise KeyError(
                    f"Task number {task_number} does not appear in \
                    the classifications."
                )
            count_results = Counter(all_results)
            resulting_classifications[subject_ids] = dict(count_results)

        return resulting_classifications

    def participants(
        self, workflow_id: int, by_workflow: bool = False
    ) -> Union[dict, list]:
        """
        Return a list of logged-in participant names who contributed to the
        specified workflow.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        workflow_id : int
            The ID of the workflow to retrieve participants for.
        by_workflow : bool, optional
            Whether to return a dictionary with all workflows and their
            participants or just a list of participants for the specified
            workflow, by default ``False``.

        Returns
        -------
        Union[dict, list]
            If ``by_workflow`` is ``False`` (default), return a list of all
            logged- in participants for the specified workflow. If
            ``by_workflow`` is ``True``, return a dictionary with workflow IDs
            as keys and a list of logged-in participants as values.
        """

        if not self._participants:
            self._participants = {
                workflow_id: list(
                    {
                        name
                        for name in rows.user_name
                        if "not-logged-in" not in name
                    }
                )
                for workflow_id, rows in self.classifications.groupby(
                    "workflow_id"
                )
            }

        if not workflow_id and by_workflow:
            return self._participants

        if not workflow_id and not by_workflow:
            return sorted(
                list(
                    {
                        item
                        for sublist in self._participants.values()
                        for item in sublist
                    }
                )
            )

        return self._participants.get(workflow_id)

    @property
    def workflow_ids(self) -> list:
        """
        Provides a list of the workflow IDs associated with the project.

        .. seealso::

            :attr:`zoonyper.project.Project.workflows`, the attribute
            containing a pandas.DataFrame of all the workflows associated with
            the project, for which this method returns the index.

        .. versionadded:: 0.1.0

        Returns
        -------
        list
            List of workflow IDs
        """

        if not self._workflow_ids:
            self._workflow_ids = list(set(self.workflows.index))

        return self._workflow_ids

    @property
    def subject_sets(self) -> dict:
        """
        Returns a dictionary of the project's subject sets.

        .. versionadded:: 0.1.0

        Returns
        -------
        dict
            A dictionary of subject sets, where the key is the subject set ID
            and the value is a list of subjects contained within that subject
            set.
        """

        if not self._subject_sets:
            self._subject_sets = {
                subject_set_id: list({x for x in rows.index})
                for subject_set_id, rows in self.subjects.groupby(
                    "subject_set_id"
                )
            }

        return self._subject_sets

    def workflow_subjects(self, workflow_id: int) -> list:
        """
        Return a list of the subject IDs for a specific workflow ID.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        workflow_id : int
            The ID of the workflow for which you want to get the subject IDs.

        Raises
        ------
        RuntimeError
            If the `workflow_id` argument is not an integer.

        Returns
        -------
        list
            A list of subject IDs for the given workflow ID.
        """
        if not isinstance(workflow_id, int):
            raise RuntimeError("workflow_id provided must be an integer")

        return list(self.subjects.query(f"workflow_id=={workflow_id}").index)

    def download_all_subjects(
        self,
        download_dir: Optional[str] = None,
        timeout: int = 5,
        sleep: Tuple[int, int] = (2, 5),
        organize_by_workflow: bool = True,
        organize_by_subject_id: bool = True,
    ) -> Literal[True]:
        """
        Downloads all subjects for each workflow in the project.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        download_dir : str, optional
            The directory to download the subjects into. If None, defaults to
            the class attribute `download_dir`.
        timeout : int, optional
            Timeout between download attempts.
        sleep : Tuple[int, int], optional
            A tuple representing the amount of time to wait (in seconds)
            between download attempts.
        organize_by_workflow : bool
            Whether to organize downloaded subjects by workflow.
        organize_by_subject_id : bool
            Whether to organize downloaded subjects by subject ID.

        Raises
        ------
        FileNotFoundError
            If `download_dir` does not exist and cannot be created.

        RuntimeError
            If the `download_dir` is not writable.

        Returns
        -------
        True
            If all subjects were downloaded successfully.
        """

        for workflow in self.workflow_ids:
            log(f"Downloading workflow {workflow}", "INFO")
            self.download_workflow(
                workflow,
                download_dir=download_dir,
                timeout=timeout,
                sleep=sleep,
                organize_by_workflow=organize_by_workflow,
                organize_by_subject_id=organize_by_subject_id,
            )

        return True

    def download_workflow(
        self,
        workflow_id: int,
        download_dir: Optional[str] = None,
        timeout: int = 5,
        sleep: Optional[Tuple[int, int]] = (2, 5),
        organize_by_workflow: bool = True,
        organize_by_subject_id: bool = True,
    ) -> Literal[True]:
        """
        Download all files associated with a particular workflow and store
        them in the specified directory. The download directory defaults to
        the value set for the ``download_dir`` parameter in the ``Project``
        constructor.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        workflow_id : int
            The ID of the workflow to download files for.
        download_dir : str, optional
            The directory to store downloaded files in. If not specified, uses
            the `download_dir` parameter set in the `Project` constructor.
        timeout : int, optional
            The time in seconds to wait for the server to respond before
            timing out.
        sleep : tuple, optional
            A tuple containing the range of time to sleep for between file
            downloads, in seconds.
        organize_by_workflow : bool, optional
            Whether to organize downloaded files by workflow ID.
        organize_by_subject_id : bool, optional
            Whether to organize downloaded files by subject ID.

        Raises
        ------
        SyntaxError
            If ``workflow_id`` is not an integer.

        Returns
        -------
        True
            Returns ``True`` if all files have been successfully downloaded and
            saved.
        """
        if not download_dir:
            download_dir = self.download_dir

        Path(download_dir).mkdir(parents=True, exist_ok=True)

        if not isinstance(workflow_id, int):
            raise SyntaxError("workflow_id provided must be an integer")

        subjects_to_download = {
            subject: self.subject_urls[subject]
            for subject in self.workflow_subjects(workflow_id)
        }

        # Setup all directories first
        for subject_id, urls in subjects_to_download.items():
            current_dir = get_current_dir(
                download_dir,
                organize_by_workflow,
                organize_by_subject_id,
                workflow_id,
                subject_id,
            )
            if not current_dir.exists():
                current_dir.mkdir(parents=True)

        for subject_id, urls in tqdm(subjects_to_download.items()):
            current_dir = get_current_dir(
                download_dir,
                organize_by_workflow,
                organize_by_subject_id,
                workflow_id,
                subject_id,
            )

            has_downloaded = False

            if not current_dir.exists():
                current_dir.mkdir(parents=True)

            for url in urls:
                file_name = url.split("/")[-1]
                save_file = Path(current_dir / Path(file_name))
                if not save_file.exists():
                    r = requests.get(url, timeout=timeout)
                    save_file.write_bytes(r.content)
                    has_downloaded = True

            if has_downloaded and isinstance(sleep, tuple):
                time.sleep(random.randint(*sleep))

        return True

    @property
    def inactive_workflow_ids(self) -> list:
        """
        Get a sorted list of unique workflow IDs, marked as inactive in the
        project's ``workflows`` DataFrame.

        .. versionadded:: 0.1.0

        Returns
        -------
        list
            A list of inactive workflow IDs.
        """
        lst = list(
            set(
                workflow_id  # TODO: add str() here? Test...
                for workflow_id, _ in self.workflows.query(
                    "active==False"
                ).iterrows()
            )
        )

        return sorted(lst)

    def get_workflow_timelines(self, include_active: bool = True) -> list:
        """
        Get the start and end dates for each workflow, and indicate whether
        they are active.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        include_active : bool
            A boolean indicating whether active workflows should be included
            (``True``) or only inactive workflows (``False``). Defaults to
            ``True``.

        Returns
        -------
        list of dictionaries
            A list of dictionaries, where each dictionary has keys:
            "workflow_id", "start_date", "end_date", and "active", which
            describe each workflow.
        """
        if not self._workflow_timeline:
            all_workflows = self.workflow_ids
            inactive_workflows = self.inactive_workflow_ids

            if include_active:
                workflow_id_list = all_workflows
            else:
                workflow_id_list = inactive_workflows

            for workflow_id in workflow_id_list:
                classification_dates = [
                    rows.created_at
                    for _, rows in self.classifications.query(
                        f"workflow_id=={workflow_id}"
                    ).iterrows()
                ]

                unique_dates = sorted(list(set(classification_dates)))

                if len(unique_dates):
                    self._workflow_timeline.append(
                        {
                            "workflow_id": workflow_id,
                            "start_date": unique_dates[0],
                            "end_date": unique_dates[-1],
                            "active": workflow_id not in inactive_workflows,
                        }
                    )

        return self._workflow_timeline

    def get_comments(self, include_staff: bool = True) -> pd.DataFrame:
        """
        Get comments for the project.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        include_staff : bool
            Include staff comments or not, defaults to ``True``.

        Raises
        ------
        ValueError
            If ``include_staff`` is ``False`` but ``staff`` property is not
            set (using ``set_staff`` method).

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing comments for the project
        """

        if not include_staff:
            if not self.staff:
                log(
                    "Staff is not set, so `include_staff` set to False has no \
                    effects. Use .set_staff method to enable this feature.",
                    "WARN",
                )
            query = (
                "user_login != '"
                + "' & user_login != '".join(self.staff)
                + "'"
            )
            return self.comments.query(query)

        return self.comments

    def get_subject_comments(
        self, subject_id, include_staff: bool = True
    ) -> pd.DataFrame:
        """
        Returns all comments made on a specific subject by users, including
        staff if specified.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        subject_id : int or str
            The ID of the subject you want to retrieve comments for
        include_staff : bool
            Whether or not to include comments made by staff members. If
            ``False``, only comments made by non-staff users will be returned.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing all comments made on the specified subject
        """
        query = f"focus_type=='Subject' & focus_id=={subject_id}"

        if not include_staff:
            if not self.staff:
                log(
                    "Staff is not set, so `include_staff` set to False has no \
                    effects. Use .set_staff method to enable this feature.",
                    "WARN",
                )
            query += (
                " & user_login != '"
                + "' & user_login != '".join(self.staff)
                + "'"
            )

        return self.comments.query(query)

    def set_staff(self, staff: list) -> None:
        """
        Set the staff members for the project.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        staff : list[str]
            A list of staff member usernames.

        Returns
        -------
        None
        """
        self.staff = staff

    def load_frame(self, name: str) -> pd.DataFrame:
        """
        Load the raw dataframe specified by ``name`` and return it. If it is
        not loaded yet, then load it first and store it in the ``_raw_frames``
        dictionary of the instance.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        name : str
            The name of the raw dataframe to load.

        Returns
        -------
        pandas.DataFrame
            The loaded raw dataframe specified by `name`.

        Raises
        ------
        SyntaxError
            If `name` is not one of the valid raw dataframe names.
        """
        if not self._raw_frames:
            self._raw_frames = {}

        if name == "classifications":
            if "classifications" not in self._raw_frames or pd.isna(
                self._raw_frames.get("classifications")
            ):
                classifications = pd.read_csv(self.classifications_path)
                classifications.set_index("classification_id", inplace=True)
                classifications = self._fix_json_cols(
                    classifications, columns=["metadata", "annotations"]
                )

                classifications = self._fix_columns(
                    classifications,
                    {
                        "gold_standard": bool,
                        "expert": bool,
                        "created_at": "date",
                    },
                )
                classifications[
                    "user_logged_in"
                ] = classifications.user_name.apply(self._user_logged_in)

                if self.redact_users:
                    classifications.user_name = (
                        classifications.user_name.apply(self.redact_username)
                    )
                    self._redacted = {}

                self._raw_frames["classifications"] = classifications

            return self._raw_frames["classifications"]

        if name == "subjects":
            if "subjects" not in self._raw_frames or pd.isna(
                self._raw_frames.get("subjects")
            ):
                subjects = pd.read_csv(self.subjects_path)
                subjects.set_index("subject_id", inplace=True)
                subjects = self._fix_json_cols(
                    subjects, columns=["metadata", "locations"]
                )

                # Drop unnecessary columns
                subjects.drop("project_id", axis=1)

                # Fill empties
                subjects.retired_at = subjects.retired_at.fillna(False)
                subjects.retirement_reason = subjects.retirement_reason.fillna(
                    ""
                )

                # Fix subjects' types
                subjects = self._fix_columns(
                    subjects,
                    {
                        "workflow_id": int,
                        "seen_before": bool,
                        "created_at": "date",
                        "updated_at": "date",
                        "retired_at": "date",
                    },
                )

                self._raw_frames["subjects"] = subjects

            return self._raw_frames["subjects"]

        if name == "workflows":
            if "workflows" not in self._raw_frames or pd.isna(
                self._raw_frames.get("workflows")
            ):
                workflows = pd.read_csv(self.workflows_path)
                workflows.set_index("workflow_id", inplace=True)

                # Fill empties
                workflows.first_task = workflows.first_task.fillna("")
                workflows.tutorial_subject_id = (
                    workflows.tutorial_subject_id.fillna("")
                )

                self._raw_frames["workflows"] = workflows

            return self._raw_frames["workflows"]

        if name == "comments":
            if "comments" not in self._raw_frames or pd.isna(
                self._raw_frames.get("comments")
            ):
                comments = pd.read_json(self.comments_path)
                comments.set_index("comment_id", inplace=True)

                comments = self._fix_columns(
                    comments,
                    {
                        "board_id": int,
                        "discussion_id": int,
                        "comment_focus_id": int,
                        "comment_user_id": int,
                        "comment_created_at": "date",
                    },
                )

                self._raw_frames["comments"] = comments

            return self._raw_frames["comments"]

        if name == "tags":
            if "tags" not in self._raw_frames or pd.isna(
                self._raw_frames.get("tags")
            ):
                tags = pd.read_json(self.tags_path)
                tags.set_index("id", inplace=True)

                # Fix tags' types
                tags = self._fix_columns(
                    tags,
                    {
                        "user_id": int,
                        "taggable_id": int,
                        "comment_id": int,
                        "created_at": "date",
                    },
                )

                self._raw_frames["tags"] = tags

            return self._raw_frames["tags"]

        raise SyntaxError()

    @property
    def frames(self) -> Dict[str, pd.DataFrame]:
        """
        Get all the frames (classifications, subjects, workflows, comments,
        and tags).

        .. versionadded:: 0.1.0

        Raises
        ------
        SyntaxError
            If the provided name is not one of the allowed options.

        Returns
        -------
        Dict[str, pandas.DataFrame]
            A dictionary with the frames' names as keys and the pandas
            DataFrame as values.
        """
        existing_frames = list(self._raw_frames.keys())

        if not all(
            [
                "classifications" in existing_frames,
                "subjects" in existing_frames,
                "workflows" in existing_frames,
                "comments" in existing_frames,
                "tags" in existing_frames,
            ]
        ):
            log("Loading all frames...", "None")

            log(
                f"--> [classifications] {self.classifications_path.name}",
                "None",
            )
            self.load_frame("classifications")

            log(f"--> [subjects] {self.subjects_path.name}", "None")
            self.load_frame("subjects")

            log(f"--> [workflows] {self.workflows_path.name}", "None")
            self.load_frame("workflows")

            log(f"--> [comments] {self.comments_path.name}", "None")
            self.load_frame("comments")

            log(f"--> [tags] {self.tags_path.name}", "None")
            self.load_frame("tags")

            # Check + warn for size excess
            for name, frame in self._raw_frames.items():
                self._check_length(frame, name)

        return self._raw_frames

    def _preprocess(self, df: pd.DataFrame, date_cols: list) -> pd.DataFrame:
        """
        Preprocesses a pandas DataFrame by converting specified columns to a
        given date format.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame to be preprocessed.
        date_cols : list
            A list of column names to be converted to the specified date
            format.

        Returns
        -------
        pandas.DataFrame
            The preprocessed DataFrame.
        """
        if not self.parse_dates:
            return df

        for col in date_cols:
            df[col] = df[col].dt.strftime(self.parse_dates)
            df[col] = df[col].fillna("")

        return df

    @property
    def comments(self) -> pd.DataFrame:
        """
        Returns a preprocessed DataFrame of the project's comments.

        .. versionadded:: 0.1.0

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the comments data.

        Notes
        -----
        - The DataFrame is loaded from the "comments" file.
        - The "comment_focus_id", "comment_user_id", "comment_created_at",
          "comment_focus_type", "comment_user_login" and "comment_body"
          columns are renamed to "focus_id", "user_id", "created_at",
          "focus_type", "user_login", and "body" respectively.
        - The "board_id", "discussion_id", "board_title", "board_description"
          and "discussion_title" columns are dropped from the DataFrame.
        - Duplicate data from comments is dropped.
        - The data is preprocessed if the `parse_dates` attribute is True.
        """
        date_cols = ["created_at"]

        if not isinstance(self._comments, pd.DataFrame):
            self._comments = self.load_frame("comments")

            # Rename inconsistently named columns on the comment frame
            self._comments = self._comments.rename(
                {
                    "comment_focus_id": "focus_id",
                    "comment_user_id": "user_id",
                    "comment_created_at": "created_at",
                    "comment_focus_type": "focus_type",
                    "comment_user_login": "user_login",
                    "comment_body": "body",
                },
                axis=1,
            )

            # Drop duplicate data from comments
            self._comments = self._comments.drop(
                [
                    "board_id",
                    "discussion_id",
                    "board_title",
                    "board_description",
                    "discussion_title",
                ],
                axis=1,
            )

            # Final preprocessing
            self._comments = self._preprocess(self._comments, date_cols)

        return self._comments

    @property
    def tags(self) -> pd.DataFrame:
        """
        Returns a preprocessed DataFrame of the project's tags.

        .. versionadded:: 0.1.0

        Returns
        -------
        pandas.DataFrame
            A preprocessed DataFrame of tags.

        Notes
        -----
        The returned DataFrame is generated from the raw tags JSON data by
        performing the following steps:
            - Joining the tags and comments frames
            - Dropping duplicate information from the tags frame
            - Preprocessing date columns using the `_preprocess` method

        The DataFrame is cached to reduce load times on subsequent calls.
        """
        date_cols = ["created_at"]

        if not isinstance(self._tags, pd.DataFrame):
            self._tags = self.load_frame("tags")

            # Join tags and comments frames
            self._tags = self._tags.join(
                self.comments, on="comment_id", rsuffix="_comment"
            )

            # Drop duplicate information from tags frame
            self._tags = self._tags.drop(
                ["user_id_comment", "user_id_comment", "created_at_comment"],
                axis=1,
            )

            # Final preprocessing
            self._tags = self._preprocess(self._tags, date_cols)

        return self._tags

    @property
    def boards(self) -> pd.DataFrame:
        """
        Returns a preprocessed DataFrame of the project's discussion boards.

        .. versionadded:: 0.1.0

        Returns
        -------
        pandas.DataFrame
            A preprocessed DataFrame of discussion boards.

        Notes
        -----
        The returned DataFrame is generated from the raw comments JSON data by
        performing the following steps:
            - Extracting the boards from the comments frame
            - Dropping duplicate information from the boards frame

        The DataFrame is cached to reduce load times on subsequent calls.
        """
        date_cols = []

        if not isinstance(self._boards, pd.DataFrame):
            # Extract boards from comments frame
            self._boards = self.load_frame("comments")[
                ["board_id", "board_title", "board_description"]
            ]
            self._boards.set_index("board_id", inplace=True)
            self._boards = self._boards.drop_duplicates()

            # Final preprocessing
            self._boards = self._preprocess(self._boards, date_cols)

        return self._boards

    @property
    def discussions(self) -> pd.DataFrame:
        """
        Returns a preprocessed DataFrame of the project's discussions.

        .. versionadded:: 0.1.0

        Returns
        -------
        pandas.DataFrame
            A preprocessed DataFrame of discussions.

        Notes
        -----
        The returned DataFrame is generated from some of the raw comments JSON
        data by performing the following steps:
            - Extracting the discussions from the comments frame
            - Dropping duplicate information from the discussions frame

        The DataFrame is cached to reduce load times on subsequent calls.
        """
        date_cols = []

        if not isinstance(self._discussions, pd.DataFrame):
            # Extract discussions from comments frame
            self._discussions = self.load_frame("comments")[
                ["discussion_id", "discussion_title"]
            ]
            self._discussions.set_index("discussion_id", inplace=True)
            self._discussions = self._discussions.drop_duplicates()

            # Final preprocessing
            self._discussions = self._preprocess(self._discussions, date_cols)

        return self._discussions

    @property
    def workflows(self) -> pd.DataFrame:
        """
        Returns a DataFrame of the project's workflows.

        .. versionadded:: 0.1.0

        Returns
        -------
        pandas.DataFrame
            A DataFrame of workflows.

        The DataFrame is cached to reduce load times on subsequent calls.
        """
        date_cols = []

        if not isinstance(self._workflows, pd.DataFrame):
            self._workflows = self.load_frame("workflows")

            # Final preprocessing
            self._workflows = self._preprocess(self._workflows, date_cols)

        return self._workflows

    @property
    def subjects(self) -> pd.DataFrame:
        """
        Returns a preprocessed DataFrame of the project's subjects.

        Issues a warning for non-disambiguated subjects if the
        :meth:`zoonyper.project.Project.disambiguate_subjects` method has not
        been run on the project instance.

        .. versionadded:: 0.1.0

        Returns
        -------
        pandas.DataFrame
            A preprocessed DataFrame of subjects.

        Notes
        -----
        The returned DataFrame is generated from some of the raw subjects CSV
        data by performing the following steps:
            - Extracting the nested metadata JSON from the metadata column
            - Joining the extracted metadata back on the subjects DataFrame
            - Dropping the metadata column
            - Preprocessing date columns using the `_preprocess` method

        The DataFrame is cached to reduce load times on subsequent calls.
        """
        date_cols = ["created_at", "updated_at", "retired_at"]

        if not isinstance(self._subjects, pd.DataFrame):
            self._subjects = self.load_frame("subjects")

            # Extract + process metadata
            subject_metadata = pd.json_normalize(self._subjects.metadata)
            subject_metadata.set_index(self._subjects.index, inplace=True)
            subject_metadata = subject_metadata.fillna("")

            # Join subjects and metadata back together + delete metadata
            self._subjects = self._subjects.join(subject_metadata)

            # Drop embedded metadata col from subjects
            self._subjects = self._subjects.drop("metadata", axis=1)

            # Final preprocessing
            self._subjects = self._preprocess(self._subjects, date_cols)

        if "subject_id_disambiguated" in self._subjects.columns:
            try:
                if not self.SUPPRESS_WARN:
                    log(
                        "Note that the subject IDs have been disambiguated \
                        and the information can be found in the \
                        `subject_id_disambiguated` column.",
                        "INFO",
                    )
            except AttributeError:
                log(
                    "Note that the subject IDs have been disambiguated and \
                    the information can be found in the \
                    `subject_id_disambiguated` column.",
                    "INFO",
                )
        else:
            try:
                if not self.SUPPRESS_WARN:
                    log(
                        "Note that the subject IDs have not yet been \
                        disambiguated. If you want to do so, run the \
                        `.disambiguate_subjects(<download-dir>)` method.",
                        "WARN",
                    )
            except AttributeError:
                log(
                    "Note that the subject IDs have not yet been \
                    disambiguated. If you want to do so, run the \
                    `.disambiguate_subjects(<download-dir>)` method.",
                    "WARN",
                )

        return self._subjects

    @property
    def classifications(self) -> pd.DataFrame:
        """
        Returns a preprocessed DataFrame of the project's classifications.

        .. versionadded:: 0.1.0

        Returns
        -------
        pandas.DataFrame
            A preprocessed DataFrame of classifications.

        Notes
        -----
        The returned DataFrame is generated from some of the raw
        classifications CSV data by performing the following steps:
            - Extracting the nested metadata JSON from the metadata column
            - Creating a new "seconds" column, denoting the seconds it took
              for the user to classify the subject(s) and dropping the original
              data used for the operation
            - Joining the extracted metadata back on the classifications
              DataFrame
            - Extracting the nested annotations data from the annotations
              column
            - Extracting all single list values as values in the annotations
              column
            - Joining the extracted and processed annotations back on the
              classifications DataFrame
            - Reduce the length of user names, user IP addresses and session
              identifiers to the shortest possible, while maintaining
              uniqueness
            - Dropping the metadata and annotations column
            - Preprocessing date columns using the `_preprocess` method

        The DataFrame is cached to reduce load times on subsequent calls.
        """
        date_cols = ["created_at"]

        if not isinstance(self._classifications, pd.DataFrame):
            self._classifications = self.load_frame("classifications")

            # Set up classifications' metadata
            classification_metadata = pd.json_normalize(
                self._classifications.metadata
            )
            classification_metadata.set_index(
                self._classifications.index, inplace=True
            )
            classification_metadata = classification_metadata.fillna("")

            # Add new classifications' columns
            classification_metadata["seconds"] = classification_metadata.apply(
                self._get_timediff, axis=1
            )

            # Drop more columns
            classification_metadata = classification_metadata.drop(
                ["started_at", "finished_at"], axis=1
            )

            # Join classifications and metadata back together
            self._classifications = self._classifications.join(
                classification_metadata
            )

            # Set up classifications' annotations
            self._classifications.annotations = (
                self._classifications.annotations.apply(
                    self._extract_annotation_values
                )
            )
            annotations = pd.json_normalize(self._classifications.annotations)
            annotations.set_index(self._classifications.index, inplace=True)

            # Extract all single list values as single values instead
            for col in annotations.columns:
                annotations[col] = annotations[col].apply(
                    lambda x: x[0]
                    if isinstance(x, list) and len(x) == 1
                    else x
                )
                annotations[col] = annotations[col].apply(
                    lambda x: "" if isinstance(x, list) and len(x) == 0 else x
                )

            annotations = annotations.fillna("")

            # Join classifications and annotations back together
            self._classifications = self._classifications.join(annotations)

            for col in ["user_name", "user_ip", "session"]:
                self._classifications = self._max_short_col(
                    self._classifications, col
                )

            self._classifications = self._classifications.drop(
                [
                    "metadata",
                    "annotations",
                    "workflow_name",
                    "subject_data",
                    "user_id",
                ],
                axis=1,
            )

            # Final preprocessing
            self._classifications = self._preprocess(
                self._classifications, date_cols
            )

        return self._classifications

    def get_classifications_for_workflow_by_dates(
        self, workflow_id: Optional[Union[int, str]] = None
    ) -> List[Dict[str, Union[str, int]]]:
        """
        Provides a list of the number of classifications in the project at any
        given date, filtered by workflow if provided (``workflow_id``).

        .. versionadded:: 0.1.0

        Parameters
        ----------
        workflow_id : Optional[Union[int, str]]
            The workflow ID, which will filter the classifications, if
            provided, optional.

        Returns
        -------
        list[dict[str, Union[str, int]]]
            A list of dictionaries which have two keys, "date" and "close",
            where close counts the classifications present in the filtered
            classifications DataFrame at any given date in the classifications
            DataFrame's date range.
        """
        if not workflow_id:
            subframe = self.classifications
        else:
            subframe = self.classifications.query(
                f"workflow_id=={workflow_id}"
            )

        values = {
            date: len(rows) for date, rows in subframe.groupby("created_at")
        }

        all_dates = [str(x) for x in values.keys()]

        if not all_dates:
            return []

        min_date = min(all_dates)
        max_date = max(all_dates)

        date_range = pd.date_range(min_date, max_date)

        dates = [x.strftime("%Y-%m-%d") for x in date_range]

        lst, cur = [], 0
        for date in dates:
            cur += values.get(date, 0)
            lst.append({"date": date, "close": cur})

        return lst

    def get_all_classifications_by_date(self) -> Dict:
        """
        Provides a dictionary with information about the number of
        classifications in the project at any given date, provided both for
        the project overall and by workflow.

        .. versionadded:: 0.1.0

        Returns
        -------
        dict
            A dictionary with the project's workflow IDs as keys (as well as a
            cross-project, "All workflows" key) and, as values, a list of
            dictionaries for each workflow (and the entire project) which have
            two keys, "date" and "close", where close counts the
            classifications present in the filtered classifications DataFrame
            at any given date in the classifications DataFrame's date range.
        """
        dic = {
            workflow_id: self.get_classifications_for_workflow_by_dates(
                workflow_id
            )
            for workflow_id in self.workflow_ids
        }
        dic["All workflows"] = self.get_classifications_for_workflow_by_dates()

        return dic

    def plot_classifications(
        self,
        workflow_id: Union[int, str] = "",
        width: int = 15,
        height: int = 5,
    ) -> Figure:
        """
        Renders the classifications for the project or a particular workflow
        (if ``workflow_id`` is provided) as a
        :class:`matplotlib.figure.Figure`.

        .. seealso::

            :meth:`zoonyper.project.Project.get_classifications_for_workflow_by_dates`,
            the method that is used to generate the growth of classifications.
            The ``workflow_id`` passed to the ``plot_classifications`` method
            is passed on as-is.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        workflow_id : int
            Any workflow ID from the project, which can be used for filtering

        width : int
            Figure width in inches, default: 15

        height : int
            Figure height in inches, default: 5

        Raises
        ------
        SyntaxError
            If width or height are not provided as integers

        Returns
        -------
        matplotlib.figure.Figure
            A line plot showing the growth of classifications
        """
        if not isinstance(width, int) or not isinstance(height, int):
            raise SyntaxError("Width and height must be provided as integers")

        data = self.get_classifications_for_workflow_by_dates(workflow_id)

        # load DataFrame
        df = pd.DataFrame(data)
        df.date = pd.to_datetime(df.date)
        df = df.set_index("date")

        # Set plot size and return the Figure
        ax = df.plot(figsize=(width, height))
        fig = ax.get_figure()

        return fig

    @property
    def annotations_flattened(
        self,
        include_columns: List[str] = [
            "workflow_id",
            "workflow_version",
            "subject_ids",
        ],
    ) -> pd.DataFrame:
        """
        Strips the classifications down to a minimal set, preserving certain
        columns (passed as the ``include_columns`` parameter) with
        classification IDs as the index and each provided classification in
        column T0, T1, T2, etc.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        include_columns : list[str]
            The list of columns to preserve from the classifications DataFrame,
            default: ``["workflow_id", "workflow_version", "subject_ids"]``

        Raises
        ------
        NotImplementedError
            If a type of data is encountered that cannot be interpreted by the
            script

        Returns
        -------
        pandas.DataFrame
            A DataFrame with the flattened annotations.
        """

        def extract_values(x):
            if isinstance(x, str):
                try:
                    x = json.loads(x)
                except json.JSONDecodeError:
                    return x

            if isinstance(x, list):
                if all([isinstance(y, dict) for y in x]):
                    values = []
                    for _dict in x:
                        for detail in _dict.get("details"):

                            value = detail.get("value")
                            if isinstance(value, str) or isinstance(
                                value, int
                            ):
                                values.append(str(value))
                            elif not value:
                                return ""
                            elif isinstance(value, list):
                                if len(value) == 1:
                                    values.append(str(value))
                                else:
                                    values.append(
                                        ",".join([str(x) for x in value])
                                    )
                            else:
                                raise NotImplementedError(
                                    f"An error occurred interpreting: {value}"
                                )
                    return "|".join([x for x in values if x])
                else:
                    return "|".join([str(y) for y in x if y])

            elif isinstance(x, dict):
                values = []
                if len(x.get("details", [])) == 1:
                    values.append(str(x.get("details", [])[0].get("value")))

                return "|".join([x for x in values if x])
            elif isinstance(x, str):
                return x
            elif isinstance(x, int):
                return str(x)
            else:
                raise NotImplementedError(
                    f"An error occurred interpreting: {x}"
                )

        if not isinstance(self._flattened, pd.DataFrame):
            task_columns = sorted(
                [
                    x
                    for x in self.classifications.columns
                    if TASK_COLUMN.search(x)
                ]
            )

            self._flattened = self.classifications[
                include_columns + task_columns
            ]

            for column in task_columns:
                self._flattened[column] = self._flattened[column].apply(
                    extract_values
                )

        return self._flattened

    def disambiguate_subjects(
        self, downloads_directory: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Disambiguates subjects by identifying unique files based on their MD5
        hashes and assigns a unique identifier to each disambiguated subject.

        The method scans through the files in the specified
        ``downloads_directory``, computes MD5 hashes of the files, and updates
        the subjects' DataFrame with disambiguated subject IDs. If the method
        has already been run, the previously disambiguated subjects' DataFrame
        is returned.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        downloads_directory : str, optional
            The file path to an existing directory where the downloads from
            the method will be saved. If not specified, the default
            `download_dir` will be used. Defaults to ``None``.

        Raises
        ------
        RuntimeError
            If the provided ``downloads_directory`` is invalid or not provided.

            If the download directory is empty.

            If there are multiple files with the same name but different
            hashes.

            If not all subjects have been properly downloaded.

        Returns
        -------
        pd.DataFrame
            The disambiguated subjects' DataFrame with an additional
            ``"subject_id_disambiguated"`` column containing unique identifiers
            for each disambiguated subject.
        """

        def get_all_files(directory: str):
            """
            Walks through a directory and returns files.

            .. versionadded:: 0.1.0

            Parameters
            ----------
            directory : str
                The path of the directory to walk through.

            Returns
            -------
            dict
                A dictionary where the keys are file names and the values are
                lists containing the directories where the files are located.

            Notes
            -----
            Could have been done with :meth:`pathlib.Path.rglob` method but
            this was (surprisingly) a lot faster: 525 ms vs 2.81 s.
            """

            all_files = {}

            for root, _, files in os.walk(directory, topdown=False):
                for name in files:
                    if name not in all_files:
                        all_files[name] = []

                    all_files[name].append(root)

            return all_files

        def get_md5(path: str):
            """
            Computes the MD5 hash of a file.

            .. versionadded:: 0.1.0

            Parameters
            ----------
            path : str
                The path of the file to compute the MD5 hash for.

            Returns
            -------
            str
                The computed MD5 hash in hexadecimal format.

            Notes
            -----
            The function is borrowed from https://bit.ly/3TvUrd1.
            """
            md5_hash = hashlib.md5()

            with open(path, "rb") as f:
                content = f.read()
                md5_hash.update(content)

                digest = md5_hash.hexdigest()

            return digest

        self.SUPPRESS_WARN = True
        # Ensure self._subjects is loaded
        self.subjects
        self.SUPPRESS_WARN = False

        # Check whether it can be assumed that this method has already been
        # run:
        if "subject_id_disambiguated" in self.subjects.columns:
            return self.subjects

        # Auto-setting for download_dir
        if not downloads_directory:
            downloads_directory = self.download_dir

        # Try creating the downloads_directory
        Path(downloads_directory).mkdir(parents=True, exist_ok=True)

        # Test downloads_directory's existence and validity
        if (
            not isinstance(downloads_directory, str)
            or not Path(downloads_directory).exists()
            or not Path(downloads_directory).is_dir()
        ):
            raise RuntimeError(
                "A required valid downloads directory was not provided."
            )

        # Get hashes by file
        def get_hashes_by_file() -> Dict[str, str]:
            """
            Scans the files in the downloads_directory, computes their MD5
            hashes, and returns a dictionary mapping filenames to their unique
            MD5 hashes. Raises a RuntimeError if the downloads_directory is
            empty or if there are multiple files with the same name but
            different hashes.

            Raises
            ------
            RuntimeError
                If the downloads_directory is empty or if there are multiple
                files with the same name but different hashes.

            Returns
            -------
            hashes_by_file : Dict[str, str]
                A dictionary mapping filenames to their unique MD5 hashes.
            """
            # Get all files from downloads_directory
            all_files = get_all_files(downloads_directory)

            if not len(all_files):
                raise RuntimeError("Looks like download directory is empty.")

            hashes_by_file = {}
            for filename, paths in tqdm(all_files.items()):
                hashes_by_file[filename] = {
                    get_md5(os.path.join(path, filename)) for path in paths
                }

            # Test to ensure that there are not multiple files with same name
            # but different hashes
            if [x for x, y in hashes_by_file.items() if len(y) > 1]:
                raise RuntimeError(
                    "Looks like there are files with the same name that are \
                    different from one another. This should not be the case \
                    with downloaded data from Zooniverse."
                )

            # Extract the unique hash
            hashes_by_file = {x: list(y)[0] for x, y in hashes_by_file.items()}

            # (Can be removed)
            # hashes = list(hashes_by_file.values())
            # doubles = [x for x in Counter(hashes).most_common() if x[1] > 1]

            return hashes_by_file

        self.hashes_by_file = get_hashes_by_file()

        if not isinstance(self._subjects, pd.DataFrame):
            self.subjects

        # Set up new columns to check for filenames + hashes across subjects
        self._subjects["filenames"] = self._subjects.apply(
            lambda row: [x.split("/")[-1] for x in row.locations.values()],
            axis=1,
        )
        try:
            self._subjects["hashes"] = self._subjects.apply(
                lambda row: str(
                    sorted([self.hashes_by_file[x] for x in row.filenames])
                ),
                axis=1,
            )
        except KeyError as file:
            raise RuntimeError(
                f"Looks like not all subjects have been properly downloaded \
                yet. Try running .download_all_subjects() with the correct \
                download directory set. The missing file was: {file}"
            )

        # Set up a disambiguated column
        self._subjects["subject_id_disambiguated"] = ""

        # Loop through hashes and set new index =
        for _, rows in self._subjects.groupby("hashes"):
            try:
                subject_id_disambiguated += 1
            except NameError:
                subject_id_disambiguated = 1
            for ix in list({ix for ix, row in rows.iterrows()}):
                self._subjects["subject_id_disambiguated"][
                    ix
                ] = subject_id_disambiguated

        # Dropping unnecessary columns
        self._subjects = self._subjects.drop(["filenames", "hashes"], axis=1)

        # Reorganise so "subject_id_disambiguated" comes first of columns
        self._subjects = self._subjects[
            ["subject_id_disambiguated"]
            + [
                x
                for x in self._subjects.columns
                if not x == "subject_id_disambiguated"
            ]
        ]

        return self._subjects

    def get_disambiguated_subject_id(
        self, subject_id: int
    ) -> Union[List, int]:
        """
        Retrieves the disambiguated subject ID for a given subject ID.

        The method returns the disambiguated subject ID associated with the
        provided subject ID. It raises a RuntimeError if the subjects have not
        been disambiguated using the
        :meth:`zoonyper.project.Project.disambiguate_subjects` method.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        subject_id : int
            The subject ID for which the disambiguated subject ID is to be
            retrieved.

        Raises
        ------
        RuntimeError
            If the subjects have not been disambiguated using the
            :meth:`zoonyper.project.Project.disambiguate_subjects` method.

        Returns
        -------
        Union[List, int]
            The disambiguated subject ID associated with the provided subject
            ID. If the subject ID is not found in the subjects DataFrame,
            returns 0. If multiple disambiguated subject IDs are associated
            with the provided subject ID, returns a list of unique
            disambiguated subject IDs.
        """
        self.SUPPRESS_WARN = True  # Set warning suppression

        # Ensure subjects is set up
        self.subjects

        if "subject_id_disambiguated" not in self.subjects.columns:
            self.SUPPRESS_WARN = False  # Reset warning suppression
            raise RuntimeError(
                "The subjects need to be disambiguated using the \
                `Project.disambiguate_subjects()` method."
            )

        try:
            _ = self.subjects["subject_id_disambiguated"][subject_id]
        except KeyError:
            log(
                f"Subject {subject_id} was not in the subjects DataFrame.",
                "WARN",
            )
            self.SUPPRESS_WARN = False  # Reset warning suppression
            return 0

        if isinstance(_, int):
            return _

        if len(list({x for x in _})) == 1:
            return [x for x in _][0]

        self.SUPPRESS_WARN = False  # Reset warning suppression
        return list({x for x in _})

    @property
    def subject_urls(self):
        """
        Retrieves a dictionary of all subject URLs matched with the subject's
        ID.

        The property compiles a list of URLs from the ``subjects`` DataFrame's
        ``locations`` column. If the ``_subject_urls`` attribute is empty, the
        method first ensures that the ``subjects`` DataFrame is set up.

        .. versionadded:: 0.1.0

        Returns
        -------
        list
            A list of all subject URLs from the ``locations`` column of the
            ``subjects`` DataFrame.
        """
        if not self._subject_urls:
            # Ensure subjects is set up
            self.subjects

            locations_series = self.subjects.locations.apply(
                lambda x: x.values()
            )

            self._subject_urls = {
                idx: list(x) for idx, x in locations_series.iteritems()
            }

        return self._subject_urls

    def get_subject_paths(
        self,
        downloads_directory: str = "",
        organize_by_workflow: bool = True,
        organize_by_subject_id: bool = True,
    ):
        """
        Retrieves a list of file paths for all subjects, organized by
        workflow and/or subject ID as specified.

        The method generates a list of file paths for each subject based on
        the given organization options. It can take into account the workflow
        ID, subject ID, and subject file name to create the file paths.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        downloads_directory : str, optional
            The file path to the directory where the subjects are downloaded.
            If not specified (default), the Project's ``download_dir``
            property will be used.

        organize_by_workflow : bool, optional
            If ``True`` (default), organizes the subject file paths by
            including the workflow ID in the path.

        organize_by_subject_id : bool, optional
            If ``True`` (default), organizes the subject file paths by
            including the subject ID (name) in the path.

        Returns
        -------
        list
            A list of file paths for all subjects, organized based on the
            specified options.
        """

        def get_locations(
            row,
            downloads_directory: str = "",
            organize_by_workflow: bool = True,
            organize_by_subject_id: bool = True,
        ):
            """
            Retrieves a list of file paths for a given subject row, organized
            by workflow and/or subject ID as specified.

            This nested function generates a list of file paths for the subject
            in the given DataFrame row based on the specified organization
            options. It takes into account the workflow ID, subject ID, and
            subject file name to create the file paths.

            .. versionadded:: 0.1.0

            Parameters
            ----------
            row : pd.Series
                A row from the subjects DataFrame containing subject metadata.

            downloads_directory : str, optional
                The file path to the directory where the subjects are
                downloaded. If not specified (default), the Project's
                ``download_dir`` property will be used.

            organize_by_workflow : bool, optional
                If ``True`` (default), organizes the subject file paths by
                including the workflow ID in the path.

            organize_by_subject_id : bool, optional
                If ``True`` (default), organizes the subject file paths by
                including the subject ID (name) in the path.

            Raises
            ------
            RuntimeError
                If an unknown error occurs in the function.

            Returns
            -------
            list
                A list of file paths for the given subject row, organized based
                on the specified options.
            """
            paths = []

            if all([not organize_by_workflow, not organize_by_subject_id]):
                # none of organize_by_workflow or organize_by_subject_id are
                # True
                paths = [
                    row.locations[x].split("/")[-1] for x in row.locations
                ]
            elif all([organize_by_workflow, organize_by_subject_id]):
                # both of organize_by_workflow or organize_by_subject_id are
                # True
                paths = [
                    f"{row.workflow_id}/{row.name}/{row.locations[x].split('/')[-1]}"  # noqa
                    for x in row.locations
                ]
            elif organize_by_workflow:
                # organize_by_workflow is True
                paths = [
                    f"{row.workflow_id}/{row.locations[x].split('/')[-1]}"
                    for x in row.locations
                ]
            elif organize_by_subject_id:
                # organize_by_subject_id is True
                paths = [
                    f"{row.name}/{row.locations[x].split('/')[-1]}"
                    for x in row.locations
                ]
            else:
                raise RuntimeError(
                    "An unknown error occurred in get_locations."
                )

            return [Path(downloads_directory) / x for x in paths]

        if downloads_directory == "":
            downloads_directory = self.download_dir

        return [
            y
            for x in self.subjects.apply(
                lambda row: get_locations(
                    row,
                    downloads_directory=downloads_directory,
                    organize_by_workflow=organize_by_workflow,
                    organize_by_subject_id=organize_by_subject_id,
                ),
                axis=1,
            )
            for y in x
        ]
