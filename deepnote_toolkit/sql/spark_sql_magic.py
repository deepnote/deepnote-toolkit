# copied from https://github.com/cryeo/sparksql-magic
# we need to modify spark imports to be lazy and return df instead of HTML table

import re

from IPython.core.magic import Magics, cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from deepnote_toolkit.logging import LoggerManager

logger = LoggerManager().get_logger()

BIND_VARIABLE_PATTERN = re.compile(r"{([A-Za-z0-9_]+)}")


@magics_class
class SparkSql(Magics):
    """
    IPython magic class for executing Spark SQL queries in notebook cells.

    Provides the `%%sparksql` cell magic that supports variable binding,
    DataFrame caching, and temporary view creation.
    """

    @needs_local_scope
    @cell_magic
    @magic_arguments()
    @argument(
        "variable", nargs="?", type=str, help="Capture dataframe in a local variable"
    )
    @argument("-c", "--cache", action="store_true", help="Cache dataframe")
    @argument(
        "-e", "--eager", action="store_true", help="Cache dataframe with eager load"
    )
    @argument("-v", "--view", type=str, help="Create or replace temporary view")
    def sparksql(self, line="", cell="", local_ns=None):
        if local_ns is None:
            local_ns = {}

        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        args = parse_argstring(self.sparksql, line)

        spark = get_instantiated_spark_session()

        df = spark.sql(bind_variables(cell, user_ns))
        if args.cache or args.eager:
            logger.debug(
                "Cache dataframe with %s load" % ("eager" if args.eager else "lazy")
            )
            df = df.cache()
            if args.eager:
                df.count()
        if args.view:
            logger.debug("Create temporary view `%s`" % args.view)
            df.createOrReplaceTempView(args.view)
        if args.variable:
            logger.debug("Capture dataframe to local variable `%s`" % args.variable)
            self.shell.user_ns.update({args.variable: df})

        return df


def bind_variables(query, user_ns):
    def fetch_variable(match):
        variable = match.group(1)
        if variable not in user_ns:
            raise NameError("variable `%s` is not defined", variable)
        return str(user_ns[variable])

    return re.sub(BIND_VARIABLE_PATTERN, fetch_variable, query)


def get_instantiated_spark_session():
    from pyspark.sql import SparkSession

    if SparkSession._instantiatedSession is None:
        raise RuntimeError(
            "Active SparkSession is not found. Please establish connection to Spark before executing %%sparksql block."
        )

    return SparkSession._instantiatedSession
