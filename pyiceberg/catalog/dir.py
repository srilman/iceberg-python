#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import pyarrow as pa

from pyiceberg.catalog import (
    WAREHOUSE_LOCATION,
    Catalog,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import (
    CommitTableResponse,
    CreateTableTransaction,
    Table,
    sorting,
)
from pyiceberg.table.update import (
    TableRequirement,
    TableUpdate,
)
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties


class DirCatalog(Catalog):
    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)
        if WAREHOUSE_LOCATION not in self.properties:
            raise ValueError(f"Missing {WAREHOUSE_LOCATION} property")

    @property
    def warehouse_path(self) -> str:
        return self.properties[WAREHOUSE_LOCATION]

    def _table_path(self, identifier: Identifier) -> str:
        wh_path = self.warehouse_path.removesuffix("/")
        return f"{wh_path}/{'/'.join(identifier)}"

    def load_table(self, identifier: str | Identifier) -> Table:
        iden = self.identifier_to_tuple(identifier)
        table_dir_path = self._table_path(iden)
        io = load_file_io(properties=self.properties)

        # Get metadata JSON file id from version-hint.text file
        version_hint_file = f"{table_dir_path}/metadata/version-hint.text"
        try:
            with io.new_input(version_hint_file).open(seekable=False) as f:
                version_hint = int(f.read().strip().decode())
        except FileNotFoundError as e:
            raise NoSuchTableError(f"No table with identifier {identifier} exists.") from e

        # Load Table from metadata JSON file path
        for metadata_loc in [
            f"{table_dir_path}/metadata/v{version_hint}.metadata.json",
            f"{table_dir_path}/metadata/v{version_hint}.gz.metadata.json",
        ]:
            if io.new_input(metadata_loc).exists():
                break
        else:
            raise FileNotFoundError(f"Metadata file not found for version `{version_hint}` of table {identifier}")

        metadata = FromInputFile.table_metadata(io.new_input(metadata_loc))
        return Table(
            identifier=iden,
            metadata=metadata,
            metadata_location=metadata_loc,
            io=load_file_io({**self.properties, **metadata.properties}),
            catalog=self,
        )

    def table_exists(self, identifier: str | Identifier) -> bool:
        iden = self.identifier_to_tuple(identifier)
        table_dir_path = self._table_path(iden)
        io = load_file_io(properties=self.properties)

        version_hint_file = f"{table_dir_path}/metadata/version-hint.text"
        return io.new_input(version_hint_file).exists()

    # All of the following functions are not implemented for a read-only catalog

    def create_namespace(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        raise NotImplementedError("DirCatalog does not support creating or updating namespaces.")

    def create_table(
        self,
        identifier: str | Identifier,
        schema: Schema | pa.Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: sorting.SortOrder = sorting.UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        raise NotImplementedError("DirCatalog does not support creating or updating tables.")

    def commit_table(
        self,
        table: Table,
        requirements: tuple[TableRequirement, ...],
        updates: tuple[TableUpdate, ...],
    ) -> CommitTableResponse:
        # TODO
        raise NotImplementedError("DirCatalog does not support creating or updating tables.")

    def create_table_transaction(
        self,
        identifier: str | Identifier,
        schema: Schema | pa.Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: sorting.SortOrder = sorting.UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> CreateTableTransaction:
        raise NotImplementedError("DirCatalog does not support creating or updating tables.")

    def register_table(self, identifier: str | Identifier, metadata_location: str) -> Table:
        raise NotImplementedError("DirCatalog does not support creating or updating tables.")

    # All of the following functions are impossible to implement with just a FileIO

    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        raise NotImplementedError("DirCatalog does not support any list_* operations")

    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        raise NotImplementedError("DirCatalog does not support any list_* operations")

    def list_views(self, namespace: str | Identifier) -> list[Identifier]:
        raise NotImplementedError("DirCatalog does not support any list_* operations")

    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        raise NotImplementedError("DirCatalog does not support namespace properties")

    def update_namespace_properties(
        self,
        namespace: str | Identifier,
        removals: set[str] | None = None,
        updates: Properties = EMPTY_DICT,
    ) -> PropertiesUpdateSummary:
        raise NotImplementedError("DirCatalog does not support namespace properties")

    def rename_table(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> Table:
        raise NotImplementedError("DirCatalog does not support rename_* operations")

    def drop_namespace(self, namespace: str | Identifier) -> None:
        raise NotImplementedError("DirCatalog does not support drop_* operations")

    def drop_table(self, identifier: str | Identifier) -> None:
        raise NotImplementedError("DirCatalog does not support drop_* operations")

    def purge_table(self, identifier: str | Identifier) -> None:
        raise NotImplementedError("DirCatalog does not support drop_* operations")

    def drop_view(self, identifier: str | Identifier) -> None:
        raise NotImplementedError("DirCatalog does not support drop_* operations")

    # The following functions may be implemented in the future

    def view_exists(self, identifier: str | Identifier) -> bool:
        raise NotImplementedError("DirCatalog does not currently support views.")
