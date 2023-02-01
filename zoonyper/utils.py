from pathlib import Path
from typing import Optional, Dict, List, Union

import pandas as pd
import hashlib
import json
import re

from .log import log


TASK_COLUMN = re.compile(r"^[T|t]\d{1,2}$")


def in_ipynb():
    try:
        cfg = get_ipython().config
        if "IPKernelApp" in cfg.keys():
            return True
        else:
            return False
    except NameError:
        return False


if in_ipynb():
    try:
        import ipywidgets
        import widgetsnbextension
    except ModuleNotFoundError:
        raise RuntimeError(
            "One or more of the required packages (ipywidgets\
            widgetsnbextension) do not appear to be installed. Run \
            `pip install ipywidgets widgetsnbextension` to ensure all \
            requirements are installed."
        )


class Utils:
    """
    Superclass to :class:`.Project`, i.e. all the methods in this class are
    inherited by the Project class. This impacts the use of some methods, see
    for example :meth:`redact_username`.

    .. data:: MAX_SIZE_OBSERVABLE

        Constant defining the size of the maximum size for Observable export.
        Default: 50MB is the max size for files on Observable, but can be set
        to other values, should Observable allow for larger files.

        .. versionadded:: 0.1.0
    """

    MAX_SIZE_OBSERVABLE = 50000000

    _redacted = {}

    def __init__(self):
        """
        Constructor method.
        """

        pass

    def redact_username(self, username: str) -> Optional[str]:
        """
        Returns a sha256 encoded string for any given string (and caches to
        speed up).

        As :class:`.Utils` is inherited by :class:`.Project`, it should be
        accessible through ``self``. Here is an example:

        .. code-block:: python

            project = Project("<path>")
            project.user_name.apply(self.redact_username)

        .. versionadded:: 0.1.0

        :param username: The username that you want to encode
        :type username: str
        :return: Username that is encoded to not be clear to human eyes
        :rtype: Optional[str]
        """

        if pd.isna(username):
            return None

        if username not in self._redacted:
            self._redacted[username] = hashlib.sha256(
                str(username).encode()
            ).hexdigest()

        return self._redacted[username]

    @staticmethod
    def trim_path(path: Union[str, Path]) -> str:
        """
        Shortcut that returns the name of the file from a file path,
        maintaining flexibility of the path's type.

        .. versionadded:: 0.1.0

        :param path: File path
        :type path: Union[str, Path]
        :return: Filename from path
        :rtype: str
        """

        if isinstance(path, str):
            path = Path(path)
        elif isinstance(path, Path):
            pass
        else:
            raise TypeError(
                "Path provided must be of type string or PurePath."
            )

        return path.name

    @staticmethod
    def camel_case(string: str) -> str:
        """
        Makes any string into a CamelCase. Adapted from
        https://www.w3resource.com/python-exercises/string/python-data-type-string-exercise-96.php # noqa

        .. versionadded:: 0.1.0

        :param string: String to make into camel case.
        :type string: str
        :return: String camel cased.
        :rtype: str
        """

        string = re.sub(r"(_|-)+", " ", string).title().replace(" ", "")

        if string == "UserIp":
            string = "UserIP"

        return "".join([string[0].lower(), string[1:]])

    @staticmethod
    def _fix_json_cols(df: pd.DataFrame, columns: List) -> pd.DataFrame:
        """
        Private function that applies `json.loads` to any given list of
        columns in a provided DataFrame (``df``). Needed because Pandas cannot
        apply this particular function to multiple columns at once.

        .. versionadded:: 0.1.0

        :param df: The DataFrame in which we want to fix the columns with
            nested JSON data
        :type df: :class:`pandas.DataFrame`
        :param columns: List of the column names to process
        :type columns: list
        :return: The DataFrame passed in the first parameter, with JSON-loaded
            content
        :rtype: :class:`pandas.DataFrame`
        """

        for col in columns:
            df[col] = df[col].apply(json.loads)

        return df

    def _fix_columns(self, df: pd.DataFrame, fix_dict: Dict) -> pd.DataFrame:
        """
        Private function that, for any DataFrame `df`, takes a dictionary
        `fix_dict` structured as `{column_name: type}`, iterates over the
        columns and applies a normative type fix.

        .. versionadded:: 0.1.0

        :param df: TODO
        :type df: :class:`pandas.DataFrame`
        :param fix_dict: TODO
        :type fix_dict: dict
        :return: TODO
        :rtype: :class:`pandas.DataFrame`
        """

        for col, type in fix_dict.items():
            if col not in df.columns:
                continue

            if type == int:
                df[col] = df[col].fillna(0).astype(type)
            elif type == bool:
                df[col] = df[col].fillna(False).astype(type)
            elif type == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce")
                """
                # TODO: Do we really want to do this here? If so, we cannot use date comparisons etc later
                if self.parse_dates:
                    df[col] = df[col].dt.strftime(self.parse_dates)
                """
            else:
                df[col] = df[col].astype(type)

        return df

    @staticmethod
    def _check_length(
        df: pd.DataFrame, category: str = "", max_length: int = 10000
    ) -> None:
        """
        Private function that checks a given DataFrame (of a certain category)
        for data in rows that exceeds a certain bytelength.

        .. versionadded:: 0.1.0

        :param df: TODO
        :type df: :class:`pandas.DataFrame`
        :param category: TODO
        :type category: str
        :param max_length: TODO
        :type max_length: int
        :return: Nothing
        :rtype: None
        """

        size_warning_rows = []

        for col in df.columns:
            for ix, row in enumerate(df[col]):
                if len(str(row)) > max_length:
                    size_warning_rows.append([ix, col])

        if size_warning_rows:
            log(
                f"[{category}]: {len(size_warning_rows)} rows have over 10kb \
                of data in some row(s) of the following columns:",
                "WARN",
            )
            columns = "- " + "\n- ".join(
                list({x[1] for x in size_warning_rows})
            )
            log(columns, "None")

        return None

    @staticmethod
    def _max_short_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
        """
        Private function that takes any column in a DataFrame and strips the
        column's values, while maintaining their uniqueness. Returns the
        DataFrame back.

        .. versionadded:: 0.1.0

        :param df: TODO
        :type df: :class:`pandas.DataFrame`
        :param col: TODO
        :type col: str
        :return: TODO
        :rtype: :class:`pandas.DataFrame`
        """

        shortened_values = set()

        i = 1
        same_length = False

        while not same_length:
            values = list({x for x in df[col]})
            shortened_values = {x[:i] for x in values}

            i += 1
            same_length = len(shortened_values) == len(values)

        char_length = len(list(shortened_values)[0])

        df[col] = df[col].apply(lambda x: x[:char_length])

        return df

    @staticmethod
    def _get_timediff(
        row: pd.Series,
        start_col: str = "started_at",
        finish_col: str = "finished_at",
    ) -> int:
        """
        Private function that returns the number of seconds in difference
        between two columns in a given row.

        .. versionadded:: 0.1.0

        :param row: TODO
        :type row: pandas.Series
        :param start_col: TODO
        :type start_col: str
        :param finish_col: TODO
        :type finish_col: str
        :return: TODO
        :rtype: int
        """

        start_data = row[start_col]
        finish_data = row[finish_col]
        try:
            start_time = pd.to_datetime(start_data)
            finish_time = pd.to_datetime(finish_data)
            return (finish_time - start_time).seconds
        except TypeError:
            log(
                f"could not interpret time difference between {start_data} \
                    and {finish_data} due to a TypeError.",
                "WARN",
            )
            return 0

    def export(
        self,
        df: pd.DataFrame,
        filename: str = "",
        filter_workflows: Optional[list] = None,
        drop_columns: Optional[list] = None,
    ) -> None:
        """
        Attempts to compress a provided DataFrame (``df``) and exports it into
        CSV format, written to a file (``filename``). If a list of workflows
        to filter is provided, only those provided in the list
        (``filter_workflows``) will be saved in the final CSV. If a list of
        column names is passed as an argument (``drop_columns``), those will be
        dropped from the DataFrame before saving.

        :term:`Export`

        .. versionadded:: 0.1.0

        :param df: The DataFrame we want to save
        :type df: :class:`pandas.DataFrame`
        :param filename: The filename to which we want to save the CSV, should
            have the file suffix ``.csv``
        :type filename: str
        :param filter_workflows: List of Zooniverse workflows to keep in the
            resulting CSV file
        :type filter_workflows: list
        :param drop_columns: List of column names to keep in the resulting CSV
            file
        :type drop_columns: list
        :return: Nothing
        :rtype: None
        """

        if filename == "":
            raise RuntimeError(
                "Export was not provided a required filename parameter."
            )

        if not isinstance(df, pd.DataFrame):
            raise RuntimeError(
                "Export was not provided a required pandas DataFrame as its \
                first parameter."
            )

        df_copy = df.copy()

        if filter_workflows:
            query = "workflow_id == "
            query += " or workflow_id == ".join(
                [str(x) for x in filter_workflows]
            )
            df_copy = df_copy.query(query)

        if drop_columns:
            df_copy = df_copy.drop(drop_columns, axis=1)

        for col in df_copy.columns:
            unique_values = {str(x) for x in df_copy[col].fillna("-") if x}
            if len(unique_values) == 1:
                val = list(unique_values)[0]
                log(
                    f'`{col}` contains only one value ("{val}"), so this \
                    column will not be exported, to save space.',
                    "INFO",
                )
                df_copy = df_copy.drop(col, axis=1)

        df_copy.to_csv(filename)

        return None

    def export_classifications(
        self,
        filename: str = "classifications.csv",
        filter_workflows: List = [],
        drop_columns: List = [],
    ) -> None:
        """
        Attempts to compress classifications and exports them into CSV format.

        .. versionadded:: 0.1.0

        :param filename: TODO
        :type filename: str
        :param filter_workflows: TODO
        :type filter_workflows: list
        :param drop_columns: TODO
        :type drop_columns: list
        :return: Nothing
        :rtype: None
        """

        self.export(
            self.classifications,
            filename=filename,
            filter_workflows=filter_workflows,
            drop_columns=drop_columns,
        )

        return None

    def export_annotations_flattened(
        self,
        filename: str = "annotations_flattened.csv",
        filter_workflows: List = [],
        drop_columns: List = [],
    ) -> None:
        """
        Attempts to compress flattened annotations and exports them into CSV
        format.

        .. versionadded:: 0.1.0

        :param filename: TODO
        :type filename: str
        :param filter_workflows: TODO
        :type filter_workflows: list
        :param drop_columns: TODO
        :type drop_columns: list
        """

        self.export(
            self.annotations_flattened,
            filename=filename,
            filter_workflows=filter_workflows,
            drop_columns=drop_columns,
        )

    def export_observable(self, directory: str = "output") -> None:
        """
        TODO

        .. versionadded:: 0.1.0

        :param directory: TODO
        :type directory: str
        :return: Nothing
        :rtype: None
        """

        Path(directory).mkdir(parents=True) if not Path(
            directory
        ).exists() else None

        # camelCase column names before exporting
        camel_classifications = self.classifications.rename(
            columns={
                col: self.camel_case(col)
                for col in self.classifications.columns
            }
        )
        camel_classifications.index = camel_classifications.index.rename(
            "classificationID"
        )

        # Export files
        self.export_annotations_flattened(
            Path(directory) / "annotations-flattened.csv",
            drop_columns=["workflow_id", "workflow_version", "subject_ids"],
        )
        self.export(
            camel_classifications,
            filename=Path(directory) / "classifications.csv",
            drop_columns=[
                x
                for x in camel_classifications.columns
                if TASK_COLUMN.search(x)
            ],  # dropping T0, T1, etc... since those are in annotations_flattened.csv
        )

        # Check for file sizes
        for cat, p in {
            "annotations": Path(directory) / "annotations-flattened.csv",
            "classifications": Path(directory) / "classifications.csv",
        }.items():
            if p.stat().st_size > self.MAX_SIZE_OBSERVABLE:
                log(
                    f"The {cat} file is too large ({round(p.stat().st_size / 1000 / 1000):,} MB). The allowed size is {round(self.MAX_SIZE_OBSERVABLE / 1000 / 1000):,} MB.",
                    "WARN",
                )

        return None


def get_current_dir(
    download_dir: str,
    organize_by_workflow: bool,
    organize_by_subject_id: bool,
    workflow_id: int = 0,
    subject_id: int = 0,
) -> Path:
    """
    Transforms a number of parameters into a Path, depending on the logic
    passed in the parameters.

    .. versionadded:: 0.1.0

    :param download_dir: TODO
    :type download_dir: str
    :param organize_by_workflow: TODO
    :type organize_by_workflow: bool
    :param organize_by_subject_id: TODO
    :type organize_by_subject_id: bool
    :param workflow_id: TODO
    :type workflow_id: int
    :param subject_id: TODO
    :type subject_id: int
    :return: TODO
    :rtype: pathlib.Path
    """

    if organize_by_workflow:
        # print("Organize by workflow set to TRUE.")
        if organize_by_subject_id:
            # print("Organize by subject_id set to TRUE.")
            return (
                Path(download_dir)
                / Path(str(workflow_id))
                / Path(str(subject_id))
            )
        else:
            # print("Organize by subject_id set to FALSE.")
            return Path(download_dir) / Path(str(workflow_id))
    else:
        # print("Organize by workflow set to FALSE.")
        if organize_by_subject_id:
            # print("Organize by subject_id set to TRUE.")
            return Path(download_dir) / Path(str(subject_id))
        else:
            # print("Organize by subject_id set to FALSE.")
            return Path(download_dir)
