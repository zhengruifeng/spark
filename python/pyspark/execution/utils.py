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

import struct

from pyspark.serializers import (
    write_int,
    read_long,
    read_bool,
    write_long,
    read_int,
)


class SpecialLengths:
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
    START_ARROW_STREAM = -6


# copied from pyspark.serializers
def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i", length)[0]


def write_int(value, stream):
    stream.write(struct.pack("!i", value))


def read_long(stream):
    length = stream.read(8)
    if not length:
        raise EOFError
    return struct.unpack("!q", length)[0]


def load_runner_conf(infile):
    from pyspark.worker_util import utf8_deserializer

    runner_conf = {}

    num_conf = read_int(infile)
    for i in range(num_conf):
        k = utf8_deserializer.loads(infile)
        v = utf8_deserializer.loads(infile)
        runner_conf[k] = v

    return runner_conf


def use_large_var_types(runner_conf):
    return runner_conf.get("spark.sql.execution.arrow.useLargeVarTypes", "false").lower() == "true"


def load_args_kwargs_offsets(infile):
    from pyspark.worker_util import utf8_deserializer

    num_arg = read_int(infile)

    args_offsets = []
    kwargs_offsets = {}
    for _ in range(num_arg):
        offset = read_int(infile)
        if read_bool(infile):
            name = utf8_deserializer.loads(infile)
            kwargs_offsets[name] = offset
        else:
            args_offsets.append(offset)

    return args_offsets, kwargs_offsets


def load_profiler(infile):
    from pyspark.worker_util import utf8_deserializer

    is_profiling = read_bool(infile)
    if is_profiling:
        profiler = utf8_deserializer.loads(infile)
    else:
        profiler = None

    return profiler


def chain(f, g):
    """chain two functions together"""
    return lambda *a: g(f(*a))


def load_chained_func(pickleSer, infile):
    from pyspark.worker_util import read_command

    chained_func = None
    for i in range(read_int(infile)):
        f, return_type = read_command(pickleSer, infile)
        if chained_func is None:
            chained_func = f
        else:
            chained_func = chain(chained_func, f)

    return chained_func, return_type


def load_profiling_func(pickleSer, infile, profiler):
    chained_func, return_type = load_chained_func(pickleSer, infile)

    # if profiler == "perf":
    #     result_id = read_long(infile)
    #
    #     if _supports_profiler(eval_type):
    #         profiling_func = wrap_perf_profiler(chained_func, result_id)
    #     else:
    #         profiling_func = chained_func
    #
    # elif profiler == "memory":
    #     result_id = read_long(infile)
    #     if _supports_profiler(eval_type) and has_memory_profiler:
    #         profiling_func = wrap_memory_profiler(chained_func, result_id)
    #     else:
    #         profiling_func = chained_func
    # else:
    #     profiling_func = chained_func

    return chained_func, return_type


# def wrap_perf_profiler(f, result_id):
#     import cProfile
#     import pstats
#
#     from pyspark.sql.profiler import ProfileResultsParam
#
#     accumulator = _deserialize_accumulator(
#         SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
#     )
#
#     def profiling_func(*args, **kwargs):
#         with cProfile.Profile() as pr:
#             ret = f(*args, **kwargs)
#         st = pstats.Stats(pr)
#         st.stream = None  # make it picklable
#         st.strip_dirs()
#
#         accumulator.add({result_id: (st, None)})
#
#         return ret
#
#     return profiling_func
#
#
# def wrap_memory_profiler(f, result_id):
#     from pyspark.sql.profiler import ProfileResultsParam
#     from pyspark.profiler import UDFLineProfilerV2
#
#     accumulator = _deserialize_accumulator(
#         SpecialAccumulatorIds.SQL_UDF_PROFIER, None, ProfileResultsParam
#     )
#
#     def profiling_func(*args, **kwargs):
#         profiler = UDFLineProfilerV2()
#
#         wrapped = profiler(f)
#         ret = wrapped(*args, **kwargs)
#         codemap_dict = {
#             filename: list(line_iterator) for filename, line_iterator in profiler.code_map.items()
#         }
#         accumulator.add({result_id: (None, codemap_dict)})
#         return ret
#
#     return profiling_func


def wrap_kwargs_support(f, args_offsets, kwargs_offsets):
    if len(kwargs_offsets):
        keys = list(kwargs_offsets.keys())

        len_args_offsets = len(args_offsets)
        if len_args_offsets > 0:

            def func(*args):
                return f(*args[:len_args_offsets], **dict(zip(keys, args[len_args_offsets:])))

        else:

            def func(*args):
                return f(**dict(zip(keys, args)))

        return func, args_offsets + [kwargs_offsets[key] for key in keys]
    else:
        return f, args_offsets


def wrap_udfs(udfs):
    def mapper(a):
        result = tuple(f(*[a[o] for o in arg_offsets]) for arg_offsets, f in udfs)
        # In the special case of a single UDF this will return a single result rather
        # than a tuple of results; this is the format that the JVM side expects.
        if len(result) == 1:
            return result[0]
        else:
            return result

    def func(_, it):
        return map(mapper, it)

    return func
