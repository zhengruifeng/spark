#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Serializers for PyArrow and pandas conversions. See `pyspark.serializers` for more details.
"""

from decimal import Decimal
from itertools import groupby
from typing import TYPE_CHECKING, Optional

import pyspark
from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.serializers import (
    Serializer,
    read_int,
    write_int,
    UTF8Deserializer,
    CPickleSerializer,
)
from pyspark.sql import Row
from pyspark.sql.conversion import LocalDataToArrowConversion, ArrowTableToRowsConversion
from pyspark.sql.pandas.types import (
    from_arrow_type,
    is_variant,
    to_arrow_type,
    _create_converter_from_pandas,
    _create_converter_to_pandas,
)
from pyspark.sql.types import (
    DataType,
    StringType,
    StructType,
    BinaryType,
    StructField,
    LongType,
    IntegerType,
)


class ArrowStreamSerializer(Serializer):
    """
    Serializes Arrow record batches as a stream.
    """

    def dump_stream(self, iterator, stream):
        import pyarrow as pa

        writer = None
        try:
            for batch in iterator:
                if writer is None:
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()

    def load_stream(self, stream):
        import pyarrow as pa

        reader = pa.ipc.open_stream(stream)
        for batch in reader:
            yield batch

    def __repr__(self):
        return "ArrowStreamSerializer"
