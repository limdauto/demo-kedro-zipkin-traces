# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Application entry point."""
from pathlib import Path
from typing import Dict

from kedro.framework.context import KedroContext, load_package_context
from kedro.framework.hooks import hook_impl
from kedro.pipeline import Pipeline
from py_zipkin.zipkin import zipkin_span
from py_zipkin.transport import SimpleHTTPTransport

from demo_traces.pipeline import create_pipelines
from demo_traces.tracing_transformers import DataSetTracingTransformer


class TransformerHooks:

    @hook_impl
    def after_catalog_created(self, catalog) -> None:
        catalog.add_transformer(DataSetTracingTransformer())


class ProjectContext(KedroContext):
    """Users can override the remaining methods from the parent class here,
    or create new ones (e.g. as required by plugins)
    """

    project_name = "demo-traces"
    # `project_version` is the version of kedro used to generate the project
    project_version = "0.16.3"
    package_name = "demo_traces"

    hooks = (TransformerHooks(),)

    def _get_pipelines(self) -> Dict[str, Pipeline]:
        return create_pipelines()

    def run(self, *args, **kwargs):
        with zipkin_span(
            service_name=self.project_name,
            span_name="pipeline_run",
            transport_handler=SimpleHTTPTransport('localhost', 9411),
            sample_rate=100.0
        ) as root:
            DataSetTracingTransformer.trace_id = root.zipkin_attrs.trace_id
            DataSetTracingTransformer.root_span_id = root.zipkin_attrs.span_id
            super().run(*args, **kwargs)


def run_package():
    # Entry point for running a Kedro project packaged with `kedro package`
    # using `python -m <project_package>.run` command.
    project_context = load_package_context(
        project_path=Path.cwd(), package_name=Path(__file__).resolve().parent.name
    )
    project_context.run()


if __name__ == "__main__":
    run_package()
