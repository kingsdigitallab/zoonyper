from pathlib import Path
import pandas as pd
import hashlib
import json


class Utils:
    """TODO"""

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

    def export_classifications(
        self,
        filename: str = "export.csv",
        filter_workflows: list = [],
        drop_columns: list = [],
    ):
        """
        Attempts to compress classifications and exports them into CSV format.
        """

        export_classifications = self.classifications.copy()

        if filter_workflows:
            query = "workflow_id == "
            query += " or workflow_id == ".join([str(x) for x in filter_workflows])
            export_classifications = export_classifications.query(query)

        if drop_columns:
            export_classifications = export_classifications.drop(drop_columns, axis=1)

        for col in export_classifications.columns:
            unique_values = {
                str(x) for x in export_classifications[col].fillna("-") if x
            }
            if len(unique_values) == 1:
                print(
                    f'`{col}` contains only one value ("{list(unique_values)[0]}"), so this column will not be exported, to save space.'
                )
                export_classifications = export_classifications.drop(col, axis=1)

        export_classifications.to_csv(filename)

        return True

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
            print(
                f"Warning [{category}]: {len(size_warning_rows)} rows have over 10kb of data in some row(s) of the following columns:"
            )
            print("- " + "\n- ".join(list({x[1] for x in size_warning_rows})))

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
            print(
                f"Warning: could not interpret time difference between {row[start_col]} and {row[finish_col]} due to a TypeError."
            )
            return 0
