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

from pyspark.execution.serializers import ArrowStreamSerializer
from pyspark.execution.utils import (
    SpecialLengths,
    read_int,
    write_int,
    load_runner_conf,
    load_profiler,
    load_args_kwargs_offsets,
    load_profiling_func,
    wrap_kwargs_support,
    wrap_udfs,
    use_large_var_types,
)
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.util import fail_on_stopiteration
from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError


class ScalarArrowUDF:
    @staticmethod
    def load_udfs(pickleSer, infile, eval_type):
        runner_conf = load_runner_conf(infile)

        timezone = runner_conf.get("spark.sql.session.timeZone", None)
        ser = ArrowStreamArrowUDFSerializer(timezone, True, True)
        profiler = load_profiler(infile)
        profiler = None  # always skip profiling for now
        num_udfs = read_int(infile)

        udfs = []
        for i in range(num_udfs):
            udfs.append(
                ScalarArrowUDF.load_single_udf(
                    pickleSer, infile, runner_conf, udf_index=i, profiler=profiler
                )
            )
        return wrap_udfs(udfs), None, ser, ser

    @staticmethod
    def load_single_udf(pickleSer, infile, runner_conf, udf_index, profiler):
        # assert 1 == 2

        args_offsets, kwargs_offsets = load_args_kwargs_offsets(infile)
        func, return_type = load_profiling_func(pickleSer, infile, profiler)
        func = fail_on_stopiteration(func)
        func, args_kwargs_offsets = wrap_kwargs_support(func, args_offsets, kwargs_offsets)

        arrow_return_type = to_arrow_type(
            return_type, prefers_large_types=use_large_var_types(runner_conf)
        )

        def verify_result_type(result):
            if not hasattr(result, "__len__"):
                pd_type = "pyarrow.Array"
                raise PySparkTypeError(
                    errorClass="UDF_RETURN_TYPE",
                    messageParameters={
                        "expected": pd_type,
                        "actual": type(result).__name__,
                    },
                )
            return result

        def verify_result_length(result, length):
            if len(result) != length:
                raise PySparkRuntimeError(
                    errorClass="SCHEMA_MISMATCH_FOR_PANDAS_UDF",
                    messageParameters={
                        "udf_type": "arrow_udf",
                        "expected": str(length),
                        "actual": str(len(result)),
                    },
                )
            return result

        return (
            args_kwargs_offsets,
            lambda *a: (
                verify_result_length(verify_result_type(func(*a)), len(a[0])),
                arrow_return_type,
            ),
        )


class ArrowStreamArrowUDFSerializer(ArrowStreamSerializer):
    """
    Serializer used by Python worker to evaluate Arrow UDFs
    """

    def __init__(
        self,
        timezone,
        safecheck,
        arrow_cast,
    ):
        super().__init__()
        self._timezone = timezone
        self._safecheck = safecheck
        self._arrow_cast = arrow_cast

    def _create_array(self, arr, arrow_type, arrow_cast):
        import pyarrow as pa

        assert isinstance(arr, pa.Array)
        assert isinstance(arrow_type, pa.DataType)

        # TODO: should we handle timezone here?

        if arr.type == arrow_type:
            return arr
        elif arrow_cast:
            return arr.cast(target_type=arrow_type, safe=self._safecheck)
        else:
            raise PySparkTypeError(
                "Arrow UDFs require the return type to match the expected Arrow type. "
                f"Expected: {arrow_type}, but got: {arr.type}."
            )

    def dump_stream(self, iterator, stream):
        """
        Override because Arrow UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """
        import pyarrow as pa

        def wrap_and_init_stream():
            should_write_start_length = True
            for packed in iterator:
                if len(packed) == 2 and isinstance(packed[1], pa.DataType):
                    # single array UDF in a projection
                    arrs = [self._create_array(packed[0], packed[1], self._arrow_cast)]
                else:
                    # multiple array UDFs in a projection
                    arrs = [self._create_array(t[0], t[1], self._arrow_cast) for t in packed]

                batch = pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])

                # Write the first record batch with initialization.
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False
                yield batch

        return ArrowStreamSerializer.dump_stream(self, wrap_and_init_stream(), stream)

    def __repr__(self):
        return "ArrowStreamArrowUDFSerializer"
