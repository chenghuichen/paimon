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

from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.data.variant_shredding import (
    VariantSchema,
    assemble_shredded_column,
    build_sub_field_column_expr,
    build_variant_schema,
    extract_sub_field_from_column,
    is_shredded_variant,
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.special_fields import SpecialFields


class FormatPyArrowReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Parquet or ORC file using PyArrow,
    and filters it based on the provided predicate and projection.

    When a VARIANT column is stored in the shredded Parquet format (a struct with
    ``metadata``, ``value``, and ``typed_value`` fields), this reader transparently
    reconstructs the standard ``struct<value: binary, metadata: binary>`` representation.

    Pass ``variant_sub_fields={'col': ['path.to.field']}`` to additionally extract typed
    sub-columns directly from the shredded file, avoiding full variant assembly for those
    paths.  The projected values are appended as extra columns named ``{col}.{dot_path}``.
    """

    def __init__(
        self,
        file_io: FileIO,
        file_format: str,
        file_path: str,
        read_fields: List[DataField],
        push_down_predicate: Any,
        batch_size: int = 1024,
        variant_sub_fields: Optional[Dict[str, List[str]]] = None,
    ):
        """
        Args:
            file_io:            FileIO for the storage backend.
            file_format:        ``'parquet'`` or ``'orc'``.
            file_path:          Path to the data file.
            read_fields:        Fields to project (in order).
            push_down_predicate: Optional Arrow expression predicate.
            batch_size:         Target rows per batch.
            variant_sub_fields: Maps VARIANT column name → list of dot-separated
                                sub-field paths to extract as typed columns.
                                Example: ``{'payload': ['age', 'address.city']}``
        """
        file_path_for_pyarrow = file_io.to_filesystem_path(file_path)
        self.dataset = ds.dataset(
            file_path_for_pyarrow, format=file_format, filesystem=file_io.filesystem
        )
        self._file_format = file_format
        self.read_fields = read_fields
        self._read_field_names = [f.name for f in read_fields]

        # Identify which fields exist in the file and which are missing
        file_schema = self.dataset.schema
        file_schema_names = set(file_schema.names)
        self.existing_fields = [
            f.name for f in read_fields if f.name in file_schema_names
        ]
        self.missing_fields = [
            f.name for f in read_fields if f.name not in file_schema_names
        ]

        # column name → VariantSchema for shredded columns that need assembly
        self._shredded_schemas: Dict[str, VariantSchema] = {}
        for name in self.existing_fields:
            try:
                field_type = file_schema.field(name).type
            except KeyError:
                continue
            if is_shredded_variant(field_type):
                self._shredded_schemas[name] = build_variant_schema(field_type)

        self._variant_sub_projections: List[_SubFieldProjection] = []
        if variant_sub_fields:
            for col_name, paths in variant_sub_fields.items():
                schema = self._shredded_schemas.get(col_name)
                if schema is None and col_name in file_schema_names:
                    # Column excluded from read_fields (replaced by sub-field columns) —
                    # detect the shredded schema directly from the Parquet file schema.
                    try:
                        field_type = file_schema.field(col_name).type
                        if is_shredded_variant(field_type):
                            schema = build_variant_schema(field_type)
                    except KeyError:
                        pass
                if schema is None:
                    if col_name in file_schema_names:
                        raise RuntimeError(
                            f"with_variant_sub_fields: column '{col_name}' is not stored in "
                            f"shredded Parquet format. Sub-field projection requires the table "
                            f"to be written with 'variant.shreddingSchema'."
                        )
                    continue
                for path_str in paths:
                    path = path_str.split(".")
                    expr = build_sub_field_column_expr(col_name, path, schema)
                    output_name = f"{col_name}.{path_str}"
                    self._variant_sub_projections.append(
                        _SubFieldProjection(
                            col_name=col_name,
                            output_name=output_name,
                            path=path,
                            schema=schema,
                            expr=expr,
                        )
                    )

        # For sub-field projections with a resolved ds.Expression, read the nested
        # column directly from Parquet to save IO.
        column_exprs: Dict[str, Any] = {}
        for name in self.existing_fields:
            column_exprs[name] = ds.field(name)
        for proj in self._variant_sub_projections:
            if proj.expr is not None:
                column_exprs[proj.output_name] = proj.expr

        # Use expression-based projection only when sub-field projections are present;
        # otherwise fall back to the simpler list form to avoid unnecessary overhead.
        if self._variant_sub_projections and any(
            p.expr is not None for p in self._variant_sub_projections
        ):
            scanner_columns = column_exprs
        else:
            scanner_columns = self.existing_fields

        self.reader = self.dataset.scanner(
            columns=scanner_columns,
            filter=push_down_predicate,
            batch_size=batch_size,
        ).to_reader()

        self._output_schema = (
            PyarrowFieldParser.from_paimon_schema(read_fields) if read_fields else None
        )

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            batch = self.reader.read_next_batch()

            if self._file_format == "orc" and self._output_schema is not None:
                batch = self._cast_orc_time_columns(batch)

            if self._shredded_schemas:
                batch = self._assemble_shredded_variants(batch)

            if self._variant_sub_projections:
                batch = self._attach_sub_field_columns(batch)

            if not self.missing_fields:
                return batch

            def _type_for_missing(name: str) -> pa.DataType:
                if self._output_schema is not None:
                    idx = self._output_schema.get_field_index(name)
                    if idx >= 0:
                        return self._output_schema.field(idx).type
                return pa.null()

            missing_columns = [
                pa.nulls(batch.num_rows, type=_type_for_missing(name))
                for name in self.missing_fields
            ]

            # Reconstruct the batch with all fields in the correct order
            all_columns = []
            out_fields = []
            for field_name in self._read_field_names:
                if field_name in self.existing_fields:
                    # Get the column from the existing batch
                    column_idx = self.existing_fields.index(field_name)
                    all_columns.append(batch.column(column_idx))
                    out_fields.append(batch.schema.field(column_idx))
                else:
                    # Get the column from missing fields
                    column_idx = self.missing_fields.index(field_name)
                    col_type = _type_for_missing(field_name)
                    all_columns.append(missing_columns[column_idx])
                    nullable = not SpecialFields.is_system_field(field_name)
                    out_fields.append(pa.field(field_name, col_type, nullable=nullable))
            # Create a new RecordBatch with all columns
            seen_variant_cols = set()
            for proj in self._variant_sub_projections:
                if proj.col_name in seen_variant_cols:
                    continue
                seen_variant_cols.add(proj.col_name)
                try:
                    col = batch.column(proj.col_name)
                    all_columns.append(col)
                    out_fields.append(
                        batch.schema.field(batch.schema.get_field_index(proj.col_name))
                    )
                except KeyError:
                    pass

            return pa.RecordBatch.from_arrays(all_columns, schema=pa.schema(out_fields))

        except StopIteration:
            return None

    def _assemble_shredded_variants(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Replace shredded VARIANT columns with standard struct<value, metadata>."""
        changed = False
        columns = list(batch.columns)
        fields = list(batch.schema)

        for i, f in enumerate(fields):
            if f.name in self._shredded_schemas:
                schema = self._shredded_schemas[f.name]
                new_col = assemble_shredded_column(columns[i], schema)
                columns[i] = new_col
                fields[i] = pa.field(f.name, new_col.type, nullable=f.nullable)
                changed = True

        if not changed:
            return batch
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

    def _attach_sub_field_columns(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Append a struct column per projected VARIANT column containing its typed sub-fields."""
        # Group projections by variant column name (preserving insertion order).
        from collections import OrderedDict

        by_col: dict = OrderedDict()
        for proj in self._variant_sub_projections:
            by_col.setdefault(proj.col_name, []).append(proj)

        extra_columns: List[pa.Array] = []
        extra_fields: List[pa.Field] = []

        for col_name, projs in by_col.items():
            sub_arrays: List[pa.Array] = []
            sub_fields: List[pa.Field] = []

            for proj in projs:
                try:
                    # Already read by the scanner via a nested expression.
                    col = batch.column(proj.output_name)
                except KeyError:
                    # Fallback: extract from the (still shredded) parent column in Python.
                    try:
                        parent_col = batch.column(proj.col_name)
                        col = extract_sub_field_from_column(
                            parent_col, proj.schema, proj.path
                        )
                    except KeyError:
                        col = pa.nulls(batch.num_rows)

                field_name = (
                    proj.path[-1] if len(proj.path) == 1 else ".".join(proj.path)
                )
                sub_arrays.append(col)
                sub_fields.append(pa.field(field_name, col.type, nullable=True))

            struct_col = pa.StructArray.from_arrays(sub_arrays, fields=sub_fields)
            extra_columns.append(struct_col)
            extra_fields.append(pa.field(col_name, struct_col.type, nullable=True))

        if not extra_columns:
            return batch

        all_columns = list(batch.columns) + extra_columns
        all_fields = list(batch.schema) + extra_fields
        return pa.RecordBatch.from_arrays(all_columns, schema=pa.schema(all_fields))

    def _cast_orc_time_columns(self, batch):
        """Cast int32 TIME columns back to time32('ms') when reading ORC."""
        columns = []
        fields = []
        changed = False
        for i, name in enumerate(batch.schema.names):
            col = batch.column(i)
            idx = self._output_schema.get_field_index(name)
            if (
                idx >= 0
                and pa.types.is_int32(col.type)
                and pa.types.is_time(self._output_schema.field(idx).type)
            ):
                col = col.cast(self._output_schema.field(idx).type)
                fields.append(self._output_schema.field(idx))
                changed = True
            else:
                fields.append(batch.schema.field(i))
            columns.append(col)
        if changed:
            return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))
        return batch

    def close(self):
        if self.reader is not None:
            self.reader = None


class _SubFieldProjection:
    """Metadata for a single VARIANT sub-field projection."""

    __slots__ = ("col_name", "output_name", "path", "schema", "expr")

    def __init__(
        self,
        col_name: str,
        output_name: str,
        path: List[str],
        schema: VariantSchema,
        expr,
    ):
        self.col_name = col_name
        self.output_name = output_name
        self.path = path
        self.schema = schema
        self.expr = expr
