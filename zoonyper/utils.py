from pathlib import Path
import pandas as pd
import hashlib
import json
import re

from .log import log


TASK_COLUMN = re.compile(r"^[T|t]\d{1,2}$")

# Load correct progressbar - TODO: I believe this is built into tqdm these days
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
            "One or more of the required packages (ipywidgets widgetsnbextension) do not appear to be installed. Run `pip install ipywidgets widgetsnbextension` to ensure all requirements are installed."
        )

    from tqdm import tqdm

    # from tqdm.notebook import tqdm_notebook as tqdm
    # log("loaded tqdm from tqdm.notebook", "INFO")
else:
    from tqdm import tqdm

    # log("loaded tqdm", "INFO")


class Utils:
    """TODO"""

    MAX_SIZE_OBSERVABLE = 50000000  # 50 MB is the max size for files on Observable

    _redacted = {}

    def redact_username(self, row: str) -> str:
        """
        Returns a sha256 encoded string for any given string (and caches to speed up).
        """

        if pd.isna(row):
            return None

        if not row in self._redacted:
            self._redacted[row] = hashlib.sha256(str(row).encode()).hexdigest()

        return self._redacted[row]

    @staticmethod
    def trim_path(path: str) -> str:
        return Path(path).name

    @staticmethod
    def camel_case(s):
        """Making any string into a CamelCase.
        Adapted from https://www.w3resource.com/python-exercises/string/python-data-type-string-exercise-96.php"""
        s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "")

        if s == "UserIp":
            s = "UserIP"

        return "".join([s[0].lower(), s[1:]])

    @staticmethod
    def _fix_json_cols(df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Private function that applies `json.loads` to any given list of columns. Needed because
        Pandas cannot apply this particular function to multiple columns at once.
        """

        for col in columns:
            df[col] = df[col].apply(json.loads)

        return df

    def _fix_columns(self, df: pd.DataFrame, fix_dict: dict) -> pd.DataFrame:
        """
        Private function that, for any DataFrame `df`, takes a dictionary `fix_dict` structured as
        `{column_name: type}`, iterates over the columns and applies a normative type fix.
        """

        for col, type in fix_dict.items():
            if not col in df.columns:
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
        Private function that checks a given DataFrame (of a certain category) for data in
        rows that exceeds a certain bytelength.
        """

        size_warning_rows = []

        for col in df.columns:
            for ix, row in enumerate(df[col]):
                if len(str(row)) > max_length:
                    size_warning_rows.append([ix, col])

        if size_warning_rows:
            log(
                f"[{category}]: {len(size_warning_rows)} rows have over 10kb of data in some row(s) of the following columns:",
                "WARN",
            )
            columns = "- " + "\n- ".join(list({x[1] for x in size_warning_rows}))
            log(columns, "None")

        return None

    @staticmethod
    def _max_short_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
        """
        Private function that takes any column in a DataFrame and strips the column's values,
        while maintaining their uniqueness. Returns the DataFrame back.
        """

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
        row: pd.Series, start_col: str = "started_at", finish_col: str = "finished_at"
    ) -> int:
        """
        Private function that returns the number of seconds in difference between two columns in a given row.
        """

        try:
            return (
                pd.to_datetime(row[finish_col]) - pd.to_datetime(row[start_col])
            ).seconds
        except TypeError:
            log(
                f"could not interpret time difference between {row[start_col]} and {row[finish_col]} due to a TypeError.",
                "WARN",
            )
            return 0

    def export(
        self,
        df,
        filename: str = None,
        filter_workflows: list = [],
        drop_columns: list = [],
    ) -> None:
        """
        Attempts to compress df and exports it into CSV format.
        """

        if not filename:
            raise RuntimeError("Export was not provided a required filename parameter.")

        if not isinstance(df, pd.DataFrame):
            raise RuntimeError(
                "Export was not provided a required pandas DataFrame as its first parameter."
            )

        df_copy = df.copy()

        if filter_workflows:
            query = "workflow_id == "
            query += " or workflow_id == ".join([str(x) for x in filter_workflows])
            df_copy = df_copy.query(query)

        if drop_columns:
            df_copy = df_copy.drop(drop_columns, axis=1)

        for col in df_copy.columns:
            unique_values = {str(x) for x in df_copy[col].fillna("-") if x}
            if len(unique_values) == 1:
                log(
                    f'`{col}` contains only one value ("{list(unique_values)[0]}"), so this column will not be exported, to save space.',
                    "INFO",
                )
                df_copy = df_copy.drop(col, axis=1)

        df_copy.to_csv(filename)

        return None

    def export_classifications(
        self,
        filename: str = "classifications.csv",
        filter_workflows: list = [],
        drop_columns: list = [],
    ) -> None:
        """
        Attempts to compress classifications and exports them into CSV format.
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
        filter_workflows: list = [],
        drop_columns: list = [],
    ) -> None:
        """
        Attempts to compress flattened annotations and exports them into CSV format.
        """

        self.export(
            self.annotations_flattened,
            filename=filename,
            filter_workflows=filter_workflows,
            drop_columns=drop_columns,
        )

    def export_observable(self, directory="output") -> None:
        Path(directory).mkdir(parents=True) if not Path(directory).exists() else None

        # camelCase column names before exporting
        camel_classifications = self.classifications.rename(
            columns={col: self.camel_case(col) for col in self.classifications.columns}
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
                x for x in camel_classifications.columns if TASK_COLUMN.search(x)
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
