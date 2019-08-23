# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint: disable=C,R,W
from datetime import datetime

import logging

import re
from typing import List, Optional, Tuple

from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.types import String, TypeEngine, UnicodeText

import sqlparse
from sqlparse.sql import Function, Identifier, Token, TokenList
from sqlparse.tokens import Keyword

from superset.db_engine_specs.base import BaseEngineSpec
from superset.sql_parse import ParsedQuery


class MssqlQuery(ParsedQuery):
    def __init__(self, sql_statement):
        super().__init__(sql_statement)
        self._parsed_ctes: TokenList = None
        self._parsed_cte_query: TokenList = None

        for statement in self._parsed:
            self._extract_ctes(statement)
            if self.has_cte:
                self._limit = self._extract_limit_from_query(self._parsed_cte_query)
            else:
                self._limit = self._extract_limit_from_query(statement)

    @property
    def has_cte(self) -> bool:
        return self._parsed_ctes is not None

    @property
    def ctes(self) -> str:
        if not self.has_cte:
            return ''

        cte_str = ''
        for token in self._parsed_ctes.tokens:
            cte_str += token.value
        return cte_str.strip(" \n\t")

    @property
    def cte_query(self):
        if not self.has_cte:
            return self.stripped()
        query_str = ''
        for token in self._parsed_cte_query.tokens:
            query_str += token.value
        return query_str.strip(" \n\t")

    @staticmethod
    def __is_cte(token: Token) -> bool:
        return token.ttype is Keyword.CTE

    def _extract_ctes(self, statement: TokenList):
        for (i, token) in enumerate(statement.tokens):
            if self.__is_cte(token):
                (idx, next_token) = statement.token_next(i)
                self._parsed_ctes = TokenList(tokens=statement.tokens[: idx + 1])
                self._parsed_cte_query = TokenList(tokens=statement.tokens[idx + 1 :])
                logging.info('Extracting CTEs {}'.format(self.ctes))
                logging.info('Extracting query {}'.format(self.cte_query))

    def _find_limit_token(self, statement: TokenList) -> Optional[Token]:
        idx, token = statement.token_next_by(i=(Function,))

        if idx is not None and token.token_first().value.lower() == "top":
            _, next_token = token.token_next(0)
            if not next_token:
                return None
            if (
                isinstance(next_token, sqlparse.sql.Parenthesis)
                and len(next_token.tokens) == 3
            ):
                return next_token.tokens[1]

        idx, token = statement.token_next_by(i=(Identifier,))

        if idx is not None and token.value.lower() == "top":
            _, next_token = statement.token_next(idx=idx)
            if not next_token:
                return None
            if next_token.ttype == sqlparse.tokens.Literal.Number.Integer:
                return next_token

        return None

    def _extract_limit_from_query(self, statement: TokenList) -> Optional[int]:
        """
        Extract limit clause from SQL statement.

        :param statement: SQL statement
        :return: Limit extracted from query, None if no limit present in statement
        """

        limit_token = self._find_limit_token(statement)

        if limit_token is not None:
            return int(limit_token.value)

        return None

    def get_cte_query_with_new_limit(self, new_limit: int) -> str:

        if not self._limit:
            return f"{self.ctes}, inner_qry as (\n{self.cte_query}\n)\nSELECT TOP {new_limit} * FROM inner_qry"

        statement = self._parsed_cte_query

        limit_token = self._find_limit_token(statement)

        if limit_token is not None:
            limit_token.value = new_limit

        str_res = ""
        for i in statement.tokens:
            str_res += str(i.value)
        return f"{self.ctes}{str_res}"

    def get_query_with_new_limit(self, new_limit: int) -> str:
        """
        returns the query with the specified limit.
        Does not change the underlying query

        :param new_limit: Limit to be incorporated into returned query
        :return: The original query with new limit
        """

        if self.has_cte:
            return self.get_cte_query_with_new_limit(new_limit)

        if not self._limit:
            return f"{self.ctes}\nSELECT TOP {new_limit} FROM (\n{self.stripped()}\n)"

        statement = self._parsed[0]
        limit_token = self._find_limit_token(statement)

        if limit_token is not None:
            limit_token.value = new_limit

        str_res = ""
        for i in statement.tokens:
            str_res += str(i.value)
        return str_res


class MssqlEngineSpec(BaseEngineSpec):
    engine = "mssql"
    epoch_to_dttm = "dateadd(S, {col}, '1970-01-01')"
    max_column_name_length = 128

    time_grain_functions = {
        None: "{col}",
        "PT1S": "DATEADD(second, DATEDIFF(second, '2000-01-01', {col}), '2000-01-01')",
        "PT1M": "DATEADD(minute, DATEDIFF(minute, 0, {col}), 0)",
        "PT5M": "DATEADD(minute, DATEDIFF(minute, 0, {col}) / 5 * 5, 0)",
        "PT10M": "DATEADD(minute, DATEDIFF(minute, 0, {col}) / 10 * 10, 0)",
        "PT15M": "DATEADD(minute, DATEDIFF(minute, 0, {col}) / 15 * 15, 0)",
        "PT0.5H": "DATEADD(minute, DATEDIFF(minute, 0, {col}) / 30 * 30, 0)",
        "PT1H": "DATEADD(hour, DATEDIFF(hour, 0, {col}), 0)",
        "P1D": "DATEADD(day, DATEDIFF(day, 0, {col}), 0)",
        "P1W": "DATEADD(week, DATEDIFF(week, 0, {col}), 0)",
        "P1M": "DATEADD(month, DATEDIFF(month, 0, {col}), 0)",
        "P0.25Y": "DATEADD(quarter, DATEDIFF(quarter, 0, {col}), 0)",
        "P1Y": "DATEADD(year, DATEDIFF(year, 0, {col}), 0)",
    }

    @classmethod
    def convert_dttm(cls, target_type: str, dttm: datetime) -> str:
        return "CONVERT(DATETIME, '{}', 126)".format(dttm.isoformat())

    @classmethod
    def fetch_data(cls, cursor, limit: int) -> List[Tuple]:
        data = super().fetch_data(cursor, limit)
        if data and type(data[0]).__name__ == "Row":
            data = [[elem for elem in r] for r in data]
        return data

    column_types = [
        (String(), re.compile(r"^(?<!N)((VAR){0,1}CHAR|TEXT|STRING)", re.IGNORECASE)),
        (UnicodeText(), re.compile(r"^N((VAR){0,1}CHAR|TEXT)", re.IGNORECASE)),
    ]

    @classmethod
    def get_sqla_column_type(cls, type_: str) -> Optional[TypeEngine]:
        for sqla_type, regex in cls.column_types:
            if regex.match(type_):
                return sqla_type
        return None

    @classmethod
    def column_datatype_to_string(
        cls, sqla_column_type: TypeEngine, dialect: Dialect
    ) -> str:
        datatype = super().column_datatype_to_string(sqla_column_type, dialect)
        # MSSQL returns long overflowing datatype
        # as in 'VARCHAR(255) COLLATE SQL_LATIN1_GENERAL_CP1_CI_AS'
        # and we don't need the verbose collation type
        str_cutoff = " COLLATE "
        if str_cutoff in datatype:
            datatype = datatype.split(str_cutoff)[0]
        return datatype

    @classmethod
    def apply_limit_to_sql(cls, sql: str, limit: int, database) -> str:
        """
        Alters the SQL statement to apply a LIMIT clause

        :param sql: SQL query
        :param limit: Maximum number of rows to be returned by the query
        :param database: Database instance
        :return: SQL query with limit clause
        """

        parsed_query = MssqlQuery(sql)
        sql = parsed_query.get_query_with_new_limit(limit)
        return sql

    @classmethod
    def get_limit_from_sql(cls, sql: str) -> int:
        """
        Extract limit from SQL query

        :param sql: SQL query
        :return: Value of limit clause in query
        """
        parsed_query = MssqlQuery(sql)
        return parsed_query.limit
