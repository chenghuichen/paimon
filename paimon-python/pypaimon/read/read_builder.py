################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from typing import Dict, List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.table_read import TableRead
from pypaimon.read.table_scan import TableScan
from pypaimon.schema.data_types import DataField
from pypaimon.table.special_fields import SpecialFields


class ReadBuilder:
    """Implementation of ReadBuilder for native Python reading."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self._predicate: Optional[Predicate] = None
        self._projection: Optional[List[str]] = None
        self._limit: Optional[int] = None
        self._variant_sub_fields: Optional[Dict[str, List[str]]] = None

    def with_filter(self, predicate: Predicate) -> 'ReadBuilder':
        self._predicate = predicate
        return self

    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        self._projection = projection
        return self

    def with_limit(self, limit: int) -> 'ReadBuilder':
        self._limit = limit
        return self

    def with_variant_sub_fields(self, variant_sub_fields: Dict[str, List[str]]) -> 'ReadBuilder':
        """Specify VARIANT sub-fields to project from shredded Parquet files.

        When reading shredded VARIANT columns, this enables sub-column IO: only the
        requested typed sub-columns are read from Parquet, skipping the full overflow
        binary and sibling fields.  The projected values are returned as additional
        typed columns appended to each batch, named ``{col}.{path}`` (e.g.
        ``payload.age``), with native Arrow types inferred from the shredded schema.

        Args:
            variant_sub_fields: mapping of VARIANT column name to a list of
                dot-separated sub-field paths.  Example::

                    {'payload': ['age', 'address.city']}

        Returns:
            self, for method chaining.
        """
        self._variant_sub_fields = variant_sub_fields
        return self

    def new_scan(self) -> TableScan:
        return TableScan(
            table=self.table,
            predicate=self._predicate,
            limit=self._limit
        )

    def new_read(self) -> TableRead:
        return TableRead(
            table=self.table,
            predicate=self._predicate,
            read_type=self.read_type(),
            variant_sub_fields=self._variant_sub_fields
        )

    def new_predicate_builder(self) -> PredicateBuilder:
        return PredicateBuilder(self.read_type())

    def read_type(self) -> List[DataField]:
        table_fields = self.table.fields

        if self._projection:
            if self.table.options.row_tracking_enabled():
                table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
            field_map = {field.name: field for field in table_fields}
            table_fields = [field_map[name] for name in self._projection if name in field_map]

        # Exclude VARIANT columns that will be replaced by typed sub-field columns.
        if self._variant_sub_fields:
            table_fields = [f for f in table_fields if f.name not in self._variant_sub_fields]

        return table_fields
