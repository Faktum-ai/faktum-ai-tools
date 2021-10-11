import hashlib

import math
import numpy as np
import pandas as pd
import sqlalchemy

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


def fill_null(vals: list) -> list:
    def bad(val):
        if isinstance(val, type(pd.NA)):
            return True
        if isinstance(val, float) and math.isnan(val):
            return True
        # the list of values you want to interpret as 'NULL' should be 
        # tweaked to your needs
        return val in ['NULL', np.nan, 'nan']
    return tuple(i if not bad(i) else None for i in vals)

def build_where(start_date_filter_col: str = None, start_date_filter: str = None, end_date_filter_col: str = None, end_date_filter: str = None) -> str:
    where_filter = ""
    #Build optional filter condition
    if (start_date_filter_col != None and end_date_filter_col != None):
        where_filter = f" AND [Target].[{start_date_filter_col}] >= '{start_date_filter}' AND [Target].[{end_date_filter_col}] <= '{end_date_filter}'"
    elif (start_date_filter_col != None):
        where_filter = f" AND [Target].[{start_date_filter_col}] >= '{start_date_filter}'"
    elif (end_date_filter_col != None):
        where_filter = f" AND [Target].[{end_date_filter_col}] <= '{end_date_filter}'"
    return where_filter

def upsert_dataframe(engine: sqlalchemy.engine.Engine, table_name_to_update: str, primary_key_cols: list, df: pd.DataFrame
    , start_date_filter_col: str = None, start_date_filter: str = None, end_date_filter_col: str = None, end_date_filter: str = None) -> None:
    """Upserts the dataframe as-is to the table.
    
    Upsert means UPDATE or INSERT. This function does not handles DELETEs. For delete functionality checkout `sync_dataframe_to_mssql`.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine, required
        The sqlachemy engine to use when connecting to the database.
    table_name_to_update : the name of the table to update, required
    primary_key_cols : List of column names, required
        A list of column names to match on. The rows that match are updated. The rows that are not matched are inserted.
    df : pandas.DataFrame, required
        The data to insert/update. Keep columns in the correct order, and make sure the column names match the column names in the database.
    start_datetime_filter_col: str, optional
        Target columnname of start_datetime to add filter too. Requires start_datetime_filter to have a datetime string. 
    start_datetime_filter: str, optional
        The date to filter (Target) start_datetime column from (>=). ex. '2021-01-01 12:00:00'. Must be parseable to SQL Server.
    end_datetime_filter_col: str, optional
        Target columnname of end_datetime to add filter too. Requires end_datetime_filter to have a datetime string. 
    end_datetime_filter: str, optional
        The datetime to filter (Target) end_datetime column from (<=). ex. '2021-01-01 12:00:00'. Must be parseable to SQL Server.
    Returns
    -------
    None
        Returns None if everything goes well. Raises and exception if not.
    """
    
    # building the command terms
    cols_list = df.columns.tolist()
    cols_list_query = f'[{("], [".join(cols_list))}]'
    sr_cols_list = [f'[Source].[{i}]' for i in cols_list]
    sr_cols_list_query = f'{(", ".join(sr_cols_list))}'
    up_cols_list = [f'[{i}]=[Source].[{i}]' for i in cols_list]
    up_cols_list_query = f'{", ".join(up_cols_list)}'

    # create the list of parameter indicators (?, ?, ?, etc...)
    # and the parameters, which are the values to be inserted
    # fill values that should be interpreted as "NULL" with None
    params = [fill_null(row.tolist()) for _, row in df.iterrows()]
    param_slots = '('+', '.join(['?']*len(df.columns))+')'
    
    merge_on = []
    for primary_key_col in primary_key_cols:
        merge_on.append(f'''[Target].[{primary_key_col}]=[Source].[{primary_key_col}] 
        ''')
    merge_on_str = ' AND '.join(merge_on)

    check_on = []
    for col in cols_list:
        check_on.append(f'''[Target].[{col}]<>[Source].[{col}] 
        ''')
    check_on_str = ' OR '.join(check_on)

    where_filter = build_where(start_date_filter_col, start_date_filter, end_date_filter_col, end_date_filter)

    cmd = f'''
        MERGE INTO {table_name_to_update} AS [Target]
        USING (
            SELECT {cols_list_query} 
            FROM (VALUES {param_slots}) AS s ({cols_list_query})
        ) AS [Source]
        ON {merge_on_str}
        WHEN NOT MATCHED{where_filter} THEN
            INSERT ({cols_list_query}) 
            VALUES ({sr_cols_list_query})
        WHEN MATCHED AND ({check_on_str}){where_filter} THEN 
            UPDATE SET {up_cols_list_query};
        '''
    # execute the command to merge tables
    with engine.begin() as conn:
        conn.execute(cmd, params)


def sync_dataframe_to_mssql(engine: sqlalchemy.engine.Engine, table_name_to_update: str, primary_key_cols: list, df: pd.DataFrame
    , start_date_filter_col: str = None, start_date_filter: str = None, end_date_filter_col: str = None, end_date_filter: str = None) -> None:
    """Syncs the dataframe as-is to the table. Use with caution.
    
    Sync means it will handle both INSERT, UPDATE and DELETE. 
    
    ***Use with caution!*** This will delete everything in the table, that is not matched by the `df` dataframe.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine, required
        The sqlachemy engine to use when connecting to the database.
    table_name_to_update : the name of the table to update, required
    primary_key_cols : List of column names, required
        A list of column names to match on. The rows that match are updated. The rows that are not matched are inserted.
    df : pandas.DataFrame, required
        The data to sync. Keep columns in the correct order, and make sure the column names match the column names in the database.
    start_datetime_filter_col: str, optional
        Target columnname of start_datetime to add filter too. Requires start_datetime_filter to have a datetime string. 
    start_datetime_filter: str, optional
        The date to filter (Target) start_datetime column from (>=). ex. '2021-01-01 12:00:00'. Must be parseable to SQL Server.
    end_datetime_filter_col: str, optional
        Target columnname of end_datetime to add filter too. Requires end_datetime_filter to have a datetime string. 
    end_datetime_filter: str, optional
        The datetime to filter (Target) end_datetime column from (<=). ex. '2021-01-01 12:00:00'. Must be parseable to SQL Server.

    Returns
    -------
    None
        Returns None if everything goes well. Raises and exception if not.
    """
    
    # building the command terms
    cols_list = df.columns.tolist()
    cols_list_query = f'({(", ".join(cols_list))})'
    sr_cols_list = [f'Source.{i}' for i in cols_list]
    sr_cols_list_query = f'({(", ".join(sr_cols_list))})'
    up_cols_list = [f'{i}=Source.{i}' for i in cols_list]
    up_cols_list_query = f'{", ".join(up_cols_list)}'

    # create the list of parameter indicators (?, ?, ?, etc...)
    # and the parameters, which are the values to be inserted
    params = [fill_null(row.tolist()) for _, row in df.iterrows()]
    param_slots = '('+', '.join(['?']*len(df.columns))+')'
    
    merge_on = []
    for primary_key_col in primary_key_cols:
        merge_on.append(f'''Target.{primary_key_col}=Source.{primary_key_col} 
        ''')
    merge_on_str = ' AND '.join(merge_on)

    check_on = []
    for col in cols_list:
        check_on.append(f'''Target.{col}<>Source.{col} 
        ''')
    check_on_str = ' OR '.join(check_on)
    
    where_filter = build_where(start_date_filter_col, start_date_filter, end_date_filter_col, end_date_filter)

    cmd = f'''
        MERGE INTO {table_name_to_update} AS [Target]
        USING (
            SELECT {cols_list_query} 
            FROM (VALUES {param_slots}) AS s ({cols_list_query})
        ) AS [Source]
        ON {merge_on_str}
        WHEN NOT MATCHED{where_filter} THEN
            INSERT ({cols_list_query}) 
            VALUES ({sr_cols_list_query})
        WHEN MATCHED AND ({check_on_str}){where_filter} THEN 
            UPDATE SET {up_cols_list_query};
        '''

    # execute the command to merge tables
    with engine.begin() as conn:
        print('Executing UPSERT statement...', end='')
        conn.execute(cmd, params)
        print(' Done')

    # Handle DELETE's
    cmd = f'''
    SELECT {', '.join(primary_key_cols)}
    FROM {table_name_to_update}
    WHERE 1=1{where_filter};
    '''

    with engine.begin() as conn:
        print('Executing SELECT statement...', end='')
        result = conn.execute(cmd)
        rows = result.fetchall()
        print(' Done')
    
    to_be_deleted = set(rows) - set([tuple(i) for i in df[primary_key_cols].values])
    if len(to_be_deleted) > 0:
        delete_on = []
        for primary_key_col in primary_key_cols:
            delete_on.append(f'''{primary_key_col}=? 
            ''')
        delete_on_str = ' AND '.join(delete_on)

        delete_cmd = f'''
        DELETE FROM {table_name_to_update}
        WHERE {delete_on_str}'''

        # execute the delete command 
        with engine.begin() as conn:
            print('Executing DELETE statement...', end='')
            conn.execute(delete_cmd, list(to_be_deleted))
            print(' Done')
    else:
        print('Nothing to delete. Moving on.')