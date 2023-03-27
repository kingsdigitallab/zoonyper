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

        Parameters
        ----------
        username : str
            The username that you want to encode

        Returns
        -------
        str, optional
            Username that is encoded to not be clear to human eyes
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

        Parameters
        ----------
        path :  Union[str, Path]
            File path

        Returns
        -------
        str
            Filename from path
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
        Makes any string into a CamelCase.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        string : str
            String to make into camel case.

        Returns
        -------
        str
            String camel cased.

        Notes
        -----
        Adapted from http://bit.ly/3yXqKs2.
        """
        string = re.sub(r"(_|-)+", " ", string).title().replace(" ", "")

        if string == "UserIp":
            string = "UserIP"

        return "".join([string[0].lower(), string[1:]])

    @staticmethod
    def _fix_json_cols(df: pd.DataFrame, columns: List) -> pd.DataFrame:
        """
        Private helper method that applies `json.loads` to any given list of
        columns in a provided DataFrame (``df``). Needed because Pandas cannot
        apply this particular function to multiple columns at once.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame in which we want to fix the columns with nested JSON
            data
        columns : list
            List of the column names to process

        Returns
        -------
        pandas.DataFrame
            The DataFrame passed in the first parameter, with JSON-loaded
            content
        """
        for col in columns:
            df[col] = df[col].apply(json.loads)

        return df

    def _fix_columns(self, df: pd.DataFrame, fix_dict: Dict) -> pd.DataFrame:
        """
        Private helper method to fix column data types in a given
        DataFrame based on a dictionary mapping column names to their
        desired data types. This method fills missing values and coerces the
        data types accordingly.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame with columns to be fixed.
        fix_dict : dict
            A dictionary mapping column names to their desired data types.

        Returns
        -------
        pandas.DataFrame
            The modified DataFrame with the specified columns fixed.
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
        Private helper method to check the length of values in each cell of a
        given DataFrame and log a warning if any cell contains a value
        exceeding the ``max_length``. This method is useful for detecting
        large values that could cause issues when processing or storing data.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame to check for cell values exceeding
            ``max_length``.
        category : str, optional
            A string used for labeling the warning message. Default is an
            empty string (``""``).
        max_length : int, optional
            The maximum allowed length for cell values. Default is ``10000``.

        Returns
        -------
        None
            The method only logs a warning if any cell value exceeds
            ``max_length``.
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
        Private helper method to shorten the values in the specified column of
        a DataFrame to the maximum common prefix length that still maintains
        unique values. This method is useful for reducing the length of values
        in a column while preserving uniqueness.

        .. versionadded:: 0.1.0

        Returns
        -------
        pandas.DataFrame
            The modified DataFrame with the specified column values shortened.

        Example
        -------
        .. code-block:: python

            >>> data = {
            ...     'A': ['abcdef', 'abcghi', 'abcjkl']
            ... }
            >>> df = pd.DataFrame(data)
            >>> shortened_df = _max_short_col(df, col='A')
            >>> print(shortened_df)
                A
            0  abcd
            1  abcg
            2  abcj

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
        Private helper method to calculate the time difference in seconds
        between two datetime columns in a given :class:`pandas.Series` (row).
        If the datetime conversion or calculation fails, log a warning and
        return ``0``.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        row : pandas.Series
            A row from a DataFrame containing the start and finish datetime
            columns.
        start_col : str, optional
            The name of the column containing the start datetime. Default is
            ``"started_at"``.
        finish_col : str, optional
            The name of the column containing the finish datetime. Default is
            ``"finished_at"``.

        Returns
        -------
        int
            The time difference in seconds between the start and finish
            datetime values.

        Example
        -------
        .. code-block:: python

            >>> data = {
            ...     'started_at': ['2021-01-01 12:00:00', '2021-01-01 12:05:00'],
            ...     'finished_at': ['2021-01-01 12:01:00', '2021-01-01 12:09:00']
            ... }
            >>> df = pd.DataFrame(data)
            >>> timediffs = df.apply(_get_timediff, axis=1)
            >>> print(timediffs)
            0     60
            1    240
            dtype: int64

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
        Export a pandas DataFrame to a CSV file with optional filtering and
        column removal. If a column contains only one unique value, it will
        not be exported to save space.

        :term:`Export`

        .. versionadded:: 0.1.0

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame to be exported.
        filename : str
            The output CSV file name. Note: Should have the file suffix
            ``.csv``.
        filter_workflows : list, optional
            A list of Zooniverse workflow IDs to filter the DataFrame before
            exporting. Default is ``None``, which means no filtering.
        drop_columns : list, optional
            A list of column names to be removed from the DataFrame before
            exporting. Default is ``None``, which means no removal.

        Returns
        -------
        None
            The method exports the DataFrame to a CSV file and doesn't return
            any value.

        Raises
        ------
        RuntimeError
            If the required filename parameter is missing or if the first
            parameter is not a pandas DataFrame.
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
        Attempts to compress the project instance's classifications and
        exports them into CSV format.

        :term:`Export`

        .. versionadded:: 0.1.0

        Parameters
        ----------
        filename : str, optional
            The output CSV file name. Note: Should have the file suffix
            ``.csv``. Defaults to ``classifications.csv``.
        filter_workflows : list, optional
            A list of Zooniverse workflow IDs to filter the classifications
            before exporting. Default is ``None``, which means no filtering.
        drop_columns : list, optional
            A list of column names to be removed from the classifications
            DataFrame before exporting. Default is ``None``, which means no
            removal.

        Returns
        -------
        None
            The method exports the classifications DataFrame to a CSV file and
            doesn't return any value.

        Raises
        ------
        RuntimeError
            If the required filename parameter is missing.
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
        Attempts to compress the project instance's flattened annotations and
        exports them into CSV format.

        :term:`Export`

        .. versionadded:: 0.1.0

        Parameters
        ----------
        filename : str, optional
            The output CSV file name. Note: Should have the file suffix
            ``.csv``. Defaults to ``annotations_flattened.csv``.
        filter_workflows : list, optional
            A list of Zooniverse workflow IDs to filter the annotations
            before exporting. Default is ``None``, which means no filtering.
        drop_columns : list, optional
            A list of column names to be removed from the annotations
            DataFrame before exporting. Default is ``None``, which means no
            removal.

        Returns
        -------
        None
            The method exports the annotations DataFrame to a CSV file and
            doesn't return any value.

        Raises
        ------
        RuntimeError
            If the required filename parameter is missing.
        """

        self.export(
            self.annotations_flattened,
            filename=filename,
            filter_workflows=filter_workflows,
            drop_columns=drop_columns,
        )

    def export_observable(self, directory: str = "output") -> None:
        """
        Export the processed classifications and annotations data to the
        specified directory as CSV files, fit for uploading to ObservableHQ.
        Before exporting, it converts column names to camel case (camelCase).
        Finally, it checks if the output files exceed the allowed size and
        logs a warning if they do.

        :term:`Export`

        .. versionadded:: 0.1.0
        Parameters
        ----------
        directory, str, optional
            The output directory path where the CSV files will be saved.
            Default is ``"output"``.

        Returns
        -------
        None
            The method exports the data to CSV files and doesn't return any
            value.
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
            ],  # dropping T0, T1, etc... since those are in annotations_flattened.csv # noqa
        )

        # Check for file sizes
        for cat, p in {
            "annotations": Path(directory) / "annotations-flattened.csv",
            "classifications": Path(directory) / "classifications.csv",
        }.items():
            if p.stat().st_size > self.MAX_SIZE_OBSERVABLE:
                size = round(p.stat().st_size / 1000 / 1000)
                max_size = round(self.MAX_SIZE_OBSERVABLE / 1000 / 1000)
                log(
                    f"The {cat} file is too large ({size:,} MB). \
                    The max allowed size is {max_size:,} MB.",
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
    Generate a Path object representing the current directory for storing
    downloaded files based on the specified organization options. The function
    can organize files by workflow, by subject ID, or both.

    .. versionadded:: 0.1.0

    Parameters
    ----------
    download_dir : str
        The base download directory path.
    organize_by_workflow : bool
        If ``True``, organize files in subdirectories named after their
        respective workflow IDs.
    organize_by_subject_id : bool
        If ``True``, organize files in subdirectories named after their
        respective subject IDs.
    workflow_id : int, optional
        The workflow ID to be used when organizing files by workflow. Default
        is ``0``.
    subject_id : int, optional
        The subject ID to be used when organizing files by subject ID. Default
        is ``0``.

    Returns
    -------
    pathlib.Path
        The Path object representing the current directory based on the
        organization options.
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
