import hashlib
from pandas.core.indexes.api import Index

def add_checksum_column(self, id_col=None, subset=None, sep="-", inplace=False):
    """
    Return dataframe with added column "row_checksum" that md5 hash key for the row.

    Used to track updates to a column when streaming to a Kafka subject

    Parameters
    ----------
    id_col : column label, optional
        If valid column lable the id_col value is added as pre-fix to the row_checksum
    subset : column label or sequence of labels, optional
        Only consider certain columns for the hash algorithm, by
        default use all of the columns.
    sep : Seperator between id_col and checksum, optional
        Only used if id_col i set. Default "-".
    inplace : bool, default False
        Whether to add row_checksum in place or to return a copy.

    Returns
    -------
    DataFrame or None
        DataFrame with the row_checksum column added or None if ``inplace=True``.

    Examples
    --------
    Consider dataset containing ramen rating.

    >>> df = pd.DataFrame({
    ...     'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
    ...     'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
    ...     'rating': [4, 4, 3.5, 15, 5]
    ... })

    >>> df
        brand style  rating
    0  Yum Yum   cup     4.0
    1  Yum Yum   cup     4.0
    2  Indomie   cup     3.5
    3  Indomie  pack    15.0
    4  Indomie  pack     5.0

    By default, it add a hash key calculated by all columns
    >>> add_checksum_column(df)
         brand style  rating                      row_checksum
    0  Yum Yum   cup     4.0  593c621424ca8d9b333e42e3ae6461b9
    1  Yum Yum   cup     4.0  593c621424ca8d9b333e42e3ae6461b9
    2  Indomie   cup     3.5  4b463227c2fe684fc61382da04f4a6f3
    3  Indomie  pack    15.0  e2a8333602cfa1ba7050ac6f1a8cdd18
    4  Indomie  pack     5.0  f35aaf41d358cbd5fb2764e442ac3753

    To generate hash only on specific column(s), use ``subset``
    >>> add_checksum_column(df, subset=["style"])
         brand style  rating                      row_checksum
    0  Yum Yum   cup     4.0  f3f58ee455ae41da2ad5de06bf55e8de
    1  Yum Yum   cup     4.0  f3f58ee455ae41da2ad5de06bf55e8de
    2  Indomie   cup     3.5  f3f58ee455ae41da2ad5de06bf55e8de
    3  Indomie  pack    15.0  b484857901742afc9e9d4e9853596ce2
    4  Indomie  pack     5.0  b484857901742afc9e9d4e9853596ce2

    To add a index as pre-fix to the hash-key with a # as seperator
    >>> add_checksum_column(df, id_col="brand", sep="#")
         brand style  rating                              row_checksum
    0  Yum Yum   cup     4.0  Yum Yum#593c621424ca8d9b333e42e3ae6461b9
    1  Yum Yum   cup     4.0  Yum Yum#593c621424ca8d9b333e42e3ae6461b9
    2  Indomie   cup     3.5  Indomie#4b463227c2fe684fc61382da04f4a6f3
    3  Indomie  pack    15.0  Indomie#e2a8333602cfa1ba7050ac6f1a8cdd18
    4  Indomie  pack     5.0  Indomie#f35aaf41d358cbd5fb2764e442ac3753
    """

    if subset is None:
        subset = self.columns

    # Verify all columns in subset exist in the queried dataframe
    diff = Index(subset).difference(self.columns)
    if not diff.empty:
        raise KeyError(diff)
    if id_col and not id_col in self.columns:
        raise KeyError(id_col)

    result = self.copy()

    result["row_checksum"] = (
        result[subset]
        .apply(lambda x: "".join(x.astype(str)), axis=1)
        .apply(lambda value: hashlib.md5(str(value).encode("utf-8")).hexdigest())
    )

    if id_col:
        result["row_checksum"] = (
            result[id_col].astype(str) + sep + result["row_checksum"]
        )

    if inplace:
        self._update_inplace(result)
        return None
    else:
        return result
