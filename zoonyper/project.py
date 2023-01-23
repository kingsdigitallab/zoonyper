from collections import ChainMap, Counter
from pathlib import Path
from tqdm import tqdm

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
pd.options.mode.chained_assignment = None


class Project(Utils):
    """The main `Project` object that controls the entire module."""

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
        You can either pass `path` as a directory that contains all five
        required files, or a combination of all five individual files
        (`classifications_path`, `subjects_path`, `workflows_path`,
        `comments_path`, `tags_path`).

        :param path: general path to directory that contains all five required
            files
        :type path: str
        :param classifications_path: path to classifications CSV file,
            optional but must be provided if no general ``path`` provided
        :type classifications_path: str
        :param subjects_path: path to subjects CSV file, optional but must be
            provided if no general ``path`` provided
        :type subjects_path: str
        :param workflows_path: path to workflows CSV file, optional but must
            be provided if no general ``path`` provided
        :type workflows_path: str
        :param comments_path: path to JSON file for project comments, optional
            but must be provided if no general ``path`` provided
        :type comments_path: str
        :param tags_path: path to JSON file for project tags, optional but
            must be provided if no general ``path`` provided
        :type tags_path: str
        :param redact_users: boolean describing whether to obscure user names
            in the classifications table.
        :param trim_paths: boolean describing whether to trim paths in columns
            that we know contain paths.
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
                    "If a general path is provided, it must contain five files: \
                    classifications.csv, subjects.csv, workflows.csv, \
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
    def _user_logged_in(row):
        return "not-logged-in" not in row if not pd.isna(row) else False

    @staticmethod
    def _extract_annotation_values(annotation_row):
        """
        Takes an annotation row, which contains a list of tasks with values in
        dictionary {task, task_label, value} and extracts the `value` for each
        `task`, disregarding the `task_label` and returns them as a dictionary,
        for easy insertion into a DataFrame.
        """

        extracted_dictionaries = [
            {task_data.get("task"): task_data.get("value")}
            for task_data in annotation_row
        ]

        return dict(ChainMap(*extracted_dictionaries))

    def participants_count(self, workflow_id=None):
        """TODO"""

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

    def logged_in(self, workflow_id=None):
        """TODO"""

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

    def classification_counts(self, workflow_id=0, task_number=0):
        """TODO"""

        results = self.classifications.query(f"workflow_id=={workflow_id}")

        resulting_classifications = {}
        """
        TODO: #3 [testing] What happens below if subject_ids contains multiple
            IDs?
        """
        for subject_ids, rows in results.groupby("subject_ids"):
            all_results = [str(x) for x in rows[f"T{task_number}"]]
            count_results = Counter(all_results)
            resulting_classifications[subject_ids] = dict(count_results)

        return resulting_classifications

    def participants(self, workflow_id=None, by_workflow=False):
        """TODO"""

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
    def workflow_ids(self):
        """TODO"""

        if not self._workflow_ids:
            self._workflow_ids = list(set(self.workflows.index))

        return self._workflow_ids

    @property
    def subject_sets(self):
        """TODO"""

        if not self._subject_sets:
            self._subject_sets = {
                subject_set_id: list({x for x in rows.index})
                for subject_set_id, rows in self.subjects.groupby(
                    "subject_set_id"
                )
            }

        return self._subject_sets

    def workflow_subjects(self, workflow_id=None):
        if not isinstance(workflow_id, int):
            raise RuntimeError("workflow_id provided must be an integer")

        return list(self.subjects.query(f"workflow_id=={workflow_id}").index)

    def download_all_subjects(
        self,
        download_dir=None,
        timeout=5,
        sleep=(2, 5),
        organize_by_workflow=True,
        organize_by_subject_id=True,
    ) -> True:
        """
        Loops over all the unique workflow IDs and downloads the workflow
        from all of them.
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
        workflow_id=None,
        download_dir=None,
        timeout=5,
        sleep=(2, 5),
        organize_by_workflow=True,
        organize_by_subject_id=True,
    ):
        """TODO"""

        if not download_dir:
            download_dir = self.download_dir

        if not isinstance(workflow_id, int):
            raise RuntimeError(f"workflow_id provided must be an integer")

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

            # ### tqdm stops this:
            # print(f"Subject {subject_id} downloaded:")
            # print("- " + "- ".join(urls))

            if has_downloaded and isinstance(sleep, tuple):
                time.sleep(random.randint(*sleep))

    @property
    def inactive_workflow_ids(self):
        """Returns a sorted list of all inactive workflows."""

        return sorted(
            list(
                {
                    workflow_id
                    for workflow_id, _ in self.workflows.query(
                        "active==False"
                    ).iterrows()
                }
            )
        )

    def get_workflow_timelines(self, include_active=True):
        """TODO"""

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
                    for classification_id, rows in self.classifications.query(
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

    def get_comments(self, include_staff=True):
        """TODO"""
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

    def get_subject_comments(self, subject_id):
        """TODO"""
        return self.comments.query(
            f"focus_type=='Subject' & focus_id=={subject_id}"
        )

    def set_staff(self, staff):
        self.staff = staff

    def load_frame(self, name):
        """TODO"""

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

    @property
    def frames(self):
        """TODO"""

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

    def _preprocess(self, df: pd.DataFrame, date_cols: list):
        if not self.parse_dates:
            return df

        for col in date_cols:
            df[col] = df[col].dt.strftime(self.parse_dates)
            df[col] = df[col].fillna("")

        return df

    @property
    def comments(self):
        """Loading function for the comments DataFrame."""

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
    def tags(self):
        """Loading function for the tags DataFrame."""

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
    def boards(self):
        """Loading function for the boards DataFrame."""

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
    def discussions(self):
        """Loading function for the discussions DataFrame."""

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
    def workflows(self):
        """Loading function for the workflows DataFrame."""

        date_cols = []

        if not isinstance(self._workflows, pd.DataFrame):
            self._workflows = self.load_frame("workflows")

            # Final preprocessing
            self._workflows = self._preprocess(self._workflows, date_cols)

        return self._workflows

    @property
    def subjects(self):
        """Loading function for the subjects DataFrame."""

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
    def classifications(self):
        """Loading function for the classifications DataFrame."""

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

    def get_classifications_for_workflow_by_dates(self, workflow_id=None):
        """TODO"""

        if not workflow_id:
            subframe = self.classifications
        else:
            subframe = self.classifications.query(
                f"workflow_id=={workflow_id}"
            )

        values = {
            date: len(rows) for date, rows in subframe.groupby("created_at")
        }

        if list(values.keys()):
            lst = []
            cur = 0

            for date in [
                x.strftime("%Y-%m-%d")
                for x in pd.date_range(
                    min(list(values.keys())), max(list(values.keys()))
                )
            ]:
                cur += values.get(date, 0)
                lst.append({"date": date, "close": cur})

            return lst

        return []

    def get_all_classifications_by_date(self) -> dict:
        """TODO"""

        dct = {}

        workflows = {id for id, _ in self.workflows.iterrows()}
        dct = {
            workflow_id: self.get_classifications_for_workflow_by_dates(
                workflow_id
            )
            for workflow_id in workflows
        }
        dct["All workflows"] = self.get_classifications_for_workflow_by_dates()

        return dct

    def plot_classifications(self, workflow_id=None, width=15, height=5):
        df = pd.DataFrame(
            self.get_classifications_for_workflow_by_dates(workflow_id)
        )
        df.date = pd.to_datetime(df.date)
        df = df.set_index("date")
        ax = df.plot(figsize=(width, height))
        fig = ax.get_figure()
        return fig

    @property
    def annotations_flattened(
        self,
        include_columns=["workflow_id", "workflow_version", "subject_ids"],
    ) -> str:
        def extract_values(x):
            if isinstance(x, str):
                try:
                    x = json.loads(x)
                except:  # TODO: replace bare except clause
                    return x

            if isinstance(x, list):
                if all([isinstance(y, dict) for y in x]):
                    values = []
                    for _dict in x:
                        for detail in _dict.get("details"):

                            if isinstance(
                                detail.get("value"), str
                            ) or isinstance(detail.get("value"), int):
                                values.append(str(detail.get("value")))
                            elif not detail.get("value"):
                                return ""
                            elif isinstance(detail.get("value"), list):
                                if len(detail.get("value")) == 1:
                                    values.append(str(detail.get("value")))
                                else:
                                    values.append(
                                        ",".join(
                                            [
                                                str(x)
                                                for x in detail.get("value")
                                            ]
                                        )
                                    )
                            else:
                                raise RuntimeError("A bug has occurred: NONE")
                    return "|".join([x for x in values if x])
                else:
                    return "|".join([str(y) for y in x if y])

            elif isinstance(x, dict):
                values = []
                if len(x.get("details")) == 1:
                    values.append(str(x.get("details")[0].get("value")))

                return "|".join([x for x in values if x])
            elif isinstance(x, str):
                return x
            elif isinstance(x, int):
                return str(x)
            else:
                raise RuntimeError("An error occurred interpreting", x)

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

    def disambiguate_subjects(self, downloads_directory=None):
        def get_all_files(directory):
            """
            Walks through a directory and returns files. Could be done with
            pathlib.Path.rglob method but this is (surprisingly) a lot faster
            (525 ms vs 2.81 s).
            """

            all_files = {}

            for root, _, files in os.walk(directory, topdown=False):
                for name in files:
                    if name not in all_files:
                        all_files[name] = []

                    all_files[name].append(root)

            return all_files

        def get_md5(path):
            """
            From https://github.com/Living-with-machines/zooniverse-data-analysis/blob/main/identifying-double-files.ipynb
            """

            md5_hash = hashlib.md5()

            with open(path, "rb") as f:
                content = f.read()
                md5_hash.update(content)

                digest = md5_hash.hexdigest()

            return digest

        self.SUPPRESS_WARN = True
        # Ensure we have loaded self._subjects
        self.subjects
        self.SUPPRESS_WARN = False

        # Check if we can assume that this method has already been run:
        if "subject_id_disambiguated" in self._subjects.columns:
            return self._subjects

        # Auto-setting for download_dir
        if not downloads_directory:
            downloads_directory = self.download_dir

        # Test download_directory's existence and validity
        if not isinstance(downloads_directory, str) or not os.path.exists(
            downloads_directory
        ):
            raise RuntimeError(
                "A required valid downloads directory was not provided."
            )

        # Get hashes by file
        def get_hashes_by_file():
            # Get all files from dowloads_directory
            all_files = get_all_files(downloads_directory)

            if not len(all_files):
                raise RuntimeError("Looks like download directory is empty.")

            hashes_by_file = {}
            for filename, paths in tqdm(all_files.items()):
                hashes_by_file[filename] = {
                    get_md5(os.path.join(path, filename)) for path in paths
                }

            # Test to ensure that there are not multiple files with same name but different hashes
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

    def get_disambiguated_subject_id(self, subject_id):
        self.SUPPRESS_WARN = True  # Set warning suppression

        # Ensure subjects is set up
        self.subjects

        if not "subject_id_disambiguated" in self.subjects.columns:
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
        if not self._subject_urls:
            # Ensure subjects is set up
            self.subjects

            self._subject_urls = [
                y
                for x in self.subjects.locations.apply(lambda x: x.values())
                for y in x
            ]

        return self._subject_urls

    def get_subject_paths(
        self,
        downloads_directory=None,
        organize_by_workflow=True,
        organize_by_subject_id=True,
    ):
        def get_locations(
            row,
            downloads_directory="",
            organize_by_workflow=True,
            organize_by_subject_id=True,
        ):
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

        if not downloads_directory:
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
