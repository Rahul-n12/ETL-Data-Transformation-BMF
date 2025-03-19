from typing import Callable
import pandas as pd


class Storage():

    @classmethod
    def read(cls, filepath: str, **kwargs) -> pd.DataFrame:
        """Read a table into a dataframe

        Parameters
        ----------
        filepath : str
            The path to the file.

        Returns
        -------
        pd.DataFrame
            The loaded table
        **kwargs
            Extra arguments parsed to the reader
        """     

        ext = filepath.split('.')[-1]
        read = cls._get_reader(ext)

        return read(filepath, **kwargs)

    @classmethod
    def _get_reader(cls, ext: str) -> Callable:

        if ext in ['xlsx', 'xls']:
            return pd.read_excel
            #return lambda filepath, **kwargs: pd.read_excel(filepath, engine="openpyxl", **kwargs)
        elif ext == 'csv':
            return pd.read_csv
        elif ext == 'parquet':
            return pd.read_parquet
        else:
            raise ValueError(f"Don't know how to read file with extension [{ext}]")

    @classmethod
    def write(cls, df: pd.DataFrame, filepath: str, **kwargs) -> None:
        """Write a dataframe to file

        Parameters
        ----------
        df : pd.DataFrame
            The df to write
        filepath : str
            The location to save df
        **kwargs
            Extra arguments parsed to the writer
        """

        ext = filepath.split('.')[-1]

        if ext in ['xlsx', 'xls']:
            df.to_excel(filepath, index=False, **kwargs)
        elif ext == 'csv':
            df.to_csv(filepath, index=False, **kwargs)
        elif ext == 'parquet':
            df.to_parquet(filepath, **kwargs)
        else:
            raise ValueError(f"Don't know how to write file with extension [{ext}]")
 