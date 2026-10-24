from .base import RunLog, Sink, State  # noqa: F401


def make_sink(name: str, settings):
    if name == "bigquery":
        from .bigquery import BigQuerySink

        return BigQuerySink(settings)
    if name == "databricks":
        from .databricks import DatabricksSink

        return DatabricksSink(settings)
    if name == "local":
        from .local import LocalSink

        return LocalSink(settings)
    raise ValueError(f"unknown sink '{name}' (expected bigquery, databricks or local)")
