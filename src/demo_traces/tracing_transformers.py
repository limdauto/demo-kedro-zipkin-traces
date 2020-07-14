from typing import Callable, Any
from kedro.io import AbstractTransformer
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.transport import SimpleHTTPTransport
import time
import random


class DataSetTracingTransformer(AbstractTransformer):

    trace_id = None
    root_span_id = None

    def _prepare_zipkins_args(self, data_set_name, operation):
        zipkin_args = {
            "service_name": data_set_name,
            "span_name": operation,
            "transport_handler": SimpleHTTPTransport('localhost', 9411),
            "zipkin_attrs": ZipkinAttrs(
                trace_id=self.trace_id,
                span_id=generate_random_64bit_string(),
                parent_span_id=self.root_span_id,
                is_sampled=False,
                flags=1
            )
        }
        
        return zipkin_args

    def load(self, data_set_name: str, load: Callable[[], Any]) -> Any:
        zipkin_args = self._prepare_zipkins_args(data_set_name, "load")
        with zipkin_span(**zipkin_args):
            time.sleep(random.randint(1, 2))
            data = load()
        return data

    def save(self, data_set_name: str, save: Callable[[Any], None], data: Any) -> None:
        zipkin_args = self._prepare_zipkins_args(data_set_name, "save")
        with zipkin_span(**zipkin_args):
            time.sleep(random.randint(1, 2))
            save(data)
        return data

