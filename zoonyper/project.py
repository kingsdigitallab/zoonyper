from collections import ChainMap, Counter
from pathlib import Path
import pandas as pd
import random
import requests
import time

from .utils import Utils


class Project(Utils):
    """The main `Project` object that controls the entire module."""

    staff = []
    _workflow_timeline = []
    _participants = {}
    _workflow_ids = []
    _subject_sets = {}
    _subject_urls = {}

    _frames = {}

    _workflows = None
    _subjects = None
    _classifications = None
    _comments = None
    _tags = None
    _discussions = None
    _boards = None

    def __init__(
        self,
        path: str = None,
        classifications_path: str = None,
        subjects_path: str = None,
        workflows_path: str = None,
        comments_path: str = None,
        tags_path: str = None,
        redact_users: bool = True,
        trim_paths: bool = True,
        parse_dates: str = "%Y-%m-%d",
    ):
        """
        You can either pass `path` as a directory that contains all five required files, or a combination
        of all five individual files (`classifications_path`, `subjects_path`, `workflows_path`,
        `comments_path`, `tags_path`.

        path: general path to directory that contains all five required files.
        classifications_path: path to classifications CSV file
        subjects_path: path to subjects CSV file
        workflows_path: path to workflows CSV file
        comments_path: string describing path to JSON file for project comments.
        tags_path: string describing path to JSON file for project tags.
        redact_users: boolean describing whether to obscure user names in the classifications table.
        trim_paths: boolean describing whether to trim paths in columns that we know contain paths
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
                "Either paths for each file or a general path argument which contains all necessary files must be provided."
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
                    "If a general path is provided, it must contain five files: classifications.csv, subjects.csv, workflows.csv, comments.json, and tags.json"
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
        Takes an annotation row, which contains a list of tasks with values in dictionary {task, task_label, value}
        and extracts the `value` for each `task`, disregarding the `task_label` and returns them as a dictionary,
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
                for _workflow_id, rows in self.classifications.groupby("workflow_id")
                if _workflow_id == workflow_id
            ]
            if len(results) == 1:
                return results[0]
            else:
                raise RuntimeError(
                    f"No participants recorded for workflow with ID {workflow_id}"
                )

        result = {
            workflow_id: len({x for x in rows.user_name})
            for workflow_id, rows in self.classifications.groupby("workflow_id")
        }

        result["total"] = len({x for x in self.classifications.user_name})

        return result

    def logged_in(self, workflow_id=None):
        """TODO"""

        if workflow_id:
            results = [
                len([x for x in rows.user_logged_in if x])
                for _workflow_id, rows in self.classifications.groupby("workflow_id")
                if _workflow_id == workflow_id
            ]
            if len(results) == 1:
                return results[0]
            else:
                raise RuntimeError(
                    f"No participants recorded for workflow with ID {workflow_id}"
                )

        result = {
            workflow_id: len([x for x in rows.user_logged_in if x])
            for workflow_id, rows in self.classifications.groupby("workflow_id")
        }

        result["total"] = len({x for x in self.classifications.user_logged_in})

        return result

    def classification_counts(self, workflow_id=0, task_number=0):
        """TODO"""

        results = self.classifications.query(f"workflow_id=={workflow_id}")

        resulting_classifications = {}
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
                    {name for name in rows.user_name if not "not-logged-in" in name}
                )
                for workflow_id, rows in self.classifications.groupby("workflow_id")
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
                for subject_set_id, rows in self.subjects.groupby("subject_set_id")
            }

        return self._subject_sets

    @property
    def subject_urls(self):
        """TODO"""

        if not self._subject_urls:
            self._subject_urls = {
                ix: list(x.locations.values()) for ix, x in self.subjects.iterrows()
            }

        return self._subject_urls

    def workflow_subjects(self, workflow_id=None):
        if not isinstance(workflow_id, int):
            raise RuntimeError("workflow_id provided must be an integer")

        return list(self.subjects.query(f"workflow_id=={workflow_id}").index)

    def download_workflow(
        self, workflow_id=None, download_dir="downloads", timeout=5, sleep=(2, 5)
    ):
        """TODO"""

        if not isinstance(workflow_id, int):
            raise RuntimeError(f"workflow_id provided must be an integer")

        subjects_to_download = {
            subject: self.subject_urls[subject]
            for subject in self.workflow_subjects(workflow_id)
        }

        # Setup all directories first
        for subject_id, urls in subjects_to_download.items():
            current_dir = (
                Path(download_dir) / Path(str(workflow_id)) / Path(str(subject_id))
            )

            if not current_dir.exists():
                current_dir.mkdir(parents=True)

        for subject_id, urls in subjects_to_download.items():
            current_dir = (
                Path(download_dir) / Path(str(workflow_id)) / Path(str(subject_id))
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

            print(f"Subject {subject_id} downloaded:")
            print("- " + "- ".join(urls))

            if has_downloaded:
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
                print(
                    "Warning: Staff is not set, so `include_staff` set to False has no effects. Use .set_staff method to enable this feature."
                )
            query = "user_login != '" + "' & user_login != '".join(self.staff) + "'"
            return self.comments.query(query)
        return self.comments

    def get_subject_comments(self, subject_id):
        """TODO"""
        return self.comments.query(f"focus_type=='Subject' & focus_id=={subject_id}")

    def set_staff(self, staff):
        self.staff = staff

    def load_frame(self, name):
        """TODO"""

        if not self._frames:
            self._frames = {}

        if name == "classifications":
            if not "classifications" in self._frames or pd.isna(
                self._frames.get("classifications")
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
                classifications["user_logged_in"] = classifications.user_name.apply(
                    self._user_logged_in
                )

                if self.redact_users:
                    classifications.user_name = classifications.user_name.apply(
                        self.redact_username
                    )
                    self._redacted = {}

                self._frames["classifications"] = classifications

            return self._frames["classifications"]

        if name == "subjects":
            if not "subjects" in self._frames or pd.isna(self._frames.get("subjects")):
                subjects = pd.read_csv(self.subjects_path)
                subjects.set_index("subject_id", inplace=True)
                subjects = self._fix_json_cols(
                    subjects, columns=["metadata", "locations"]
                )

                # Drop unnecessary columns
                subjects.drop("project_id", axis=1)

                # Fill empties
                subjects.retired_at = subjects.retired_at.fillna(False)
                subjects.retirement_reason = subjects.retirement_reason.fillna("")

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

                self._frames["subjects"] = subjects

            return self._frames["subjects"]

        if name == "workflows":
            if not "workflows" in self._frames or pd.isna(
                self._frames.get("workflows")
            ):
                workflows = pd.read_csv(self.workflows_path)
                workflows.set_index("workflow_id", inplace=True)

                # Fill empties
                workflows.first_task = workflows.first_task.fillna("")
                workflows.tutorial_subject_id = workflows.tutorial_subject_id.fillna("")

                self._frames["workflows"] = workflows

            return self._frames["workflows"]

        if name == "comments":
            if not "comments" in self._frames or pd.isna(self._frames.get("comments")):
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

                self._frames["comments"] = comments

            return self._frames["comments"]

        if name == "tags":
            if not "tags" in self._frames or pd.isna(self._frames.get("tags")):
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

                self._frames["tags"] = tags

            return self._frames["tags"]

    @property
    def frames(self):
        """TODO"""

        existing_frames = list(self._frames.keys())

        if not all(
            [
                "classifications" in existing_frames,
                "subjects" in existing_frames,
                "workflows" in existing_frames,
                "comments" in existing_frames,
                "tags" in existing_frames,
            ]
        ):
            print("Loading all frames...")

            print(f"--> [classifications] {self.classifications_path.name}")
            self.load_frame("classifications")

            print(f"--> [subjects] {self.subjects_path.name}")
            self.load_frame("subjects")

            print(f"--> [workflows] {self.workflows_path.name}")
            self.load_frame("workflows")

            print(f"--> [comments] {self.comments_path.name}")
            self.load_frame("comments")

            print(f"--> [tags] {self.tags_path.name}")
            self.load_frame("tags")

            # Check + warn for size excess
            for name, frame in self._frames.items():
                self._check_length(frame, name)

        return self._frames

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
                ["user_id_comment", "user_id_comment", "created_at_comment"], axis=1
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

        return self._subjects

    @property
    def classifications(self):
        """Loading function for the classifications DataFrame."""

        date_cols = ["created_at"]

        if not isinstance(self._classifications, pd.DataFrame):
            self._classifications = self.load_frame("classifications")

            # Set up classifications' metadata
            classification_metadata = pd.json_normalize(self._classifications.metadata)
            classification_metadata.set_index(self._classifications.index, inplace=True)
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
            self._classifications = self._classifications.join(classification_metadata)

            # Set up classifications' annotations
            self._classifications.annotations = self._classifications.annotations.apply(
                self._extract_annotation_values
            )
            annotations = pd.json_normalize(self._classifications.annotations)
            annotations.set_index(self._classifications.index, inplace=True)

            # Extract all single list values as single values instead
            for col in annotations.columns:
                annotations[col] = annotations[col].apply(
                    lambda x: x[0] if isinstance(x, list) and len(x) == 1 else x
                )
                annotations[col] = annotations[col].apply(
                    lambda x: "" if isinstance(x, list) and len(x) == 0 else x
                )

            annotations = annotations.fillna("")

            # Join classifications and annotations back together
            self._classifications = self._classifications.join(annotations)

            for col in ["user_name", "user_ip", "session"]:
                self._classifications = self._max_short_col(self._classifications, col)

            self._classifications = self._classifications.drop(
                ["metadata", "annotations", "workflow_name", "subject_data", "user_id"],
                axis=1,
            )

            # Final preprocessing
            self._classifications = self._preprocess(self._classifications, date_cols)

        return self._classifications

    def get_classifications_for_workflow_by_dates(self, workflow_id=None):
        """TODO"""

        if not workflow_id:
            subframe = self.classifications
        else:
            subframe = self.classifications.query(f"workflow_id=={workflow_id}")

        values = {date: len(rows) for date, rows in subframe.groupby("created_at")}

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
            workflow_id: self.get_classifications_for_workflow_by_dates(workflow_id)
            for workflow_id in workflows
        }
        dct["All workflows"] = self.get_classifications_for_workflow_by_dates()

        return dct

    def plot_classifications(self, workflow_id=None, width=15, height=5):
        df = pd.DataFrame(self.get_classifications_for_workflow_by_dates(workflow_id))
        df.date = pd.to_datetime(df.date)
        df = df.set_index("date")
        ax = df.plot(figsize=(width, height))
        fig = ax.get_figure()
        return fig
