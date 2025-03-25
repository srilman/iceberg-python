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
from pathlib import Path

import pytest

from pyiceberg.io import PY_IO_IMPL
from pyiceberg.table import (
    StaticTable,
    Table,
)


def test_static_table_same_as_table(table_v2: Table, metadata_location: str) -> None:
    static_table = StaticTable.from_metadata(metadata_location)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table_v2.metadata


def test_static_table_gz_same_as_table(table_v2: Table, metadata_location_gz: str) -> None:
    static_table = StaticTable.from_metadata(metadata_location_gz)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table_v2.metadata


def test_static_table_io_does_not_exist(metadata_location: str) -> None:
    with pytest.raises(ValueError):
        StaticTable.from_metadata(metadata_location, {PY_IO_IMPL: "pyiceberg.does.not.exist.FileIO"})


def test_static_table_hint_same(table_v2: Table, metadata_location_hint: str) -> None:
    static_table = StaticTable.from_version_hint(metadata_location_hint)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table_v2.metadata


def test_static_table_hint_gz_same(table_v2: Table, metadata_location_gz_hint: str) -> None:
    static_table = StaticTable.from_version_hint(metadata_location_gz_hint)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table_v2.metadata


def test_static_table_hint_io_does_not_exist(metadata_location_hint: str) -> None:
    with pytest.raises(ValueError):
        StaticTable.from_version_hint(metadata_location_hint, {PY_IO_IMPL: "pyiceberg.does.not.exist.FileIO"})


def test_static_table_no_hint() -> None:
    with pytest.raises(FileNotFoundError):
        StaticTable.from_version_hint("/nonpath/version-hint.text")


def test_static_table_hint_no_metadata(tmp_path: Path) -> None:
    version_location = str(tmp_path / "version-hint.text")
    with open(version_location, "wb") as output:
        output.write(b"1")

    with pytest.raises(FileNotFoundError):
        StaticTable.from_version_hint(version_location)
