import os
from tfx import v1 as tfx
import urllib.request
import tempfile


PIPELINE_NAME = "keras-simple"
PIPELINE_ROOT = os.path.join("outputs", PIPELINE_NAME)
METADATA_PATH = os.path.join("metadata", PIPELINE_NAME, "metadata.db")
SERVING_MODEL_DIR = os.path.join("outputs", PIPELINE_NAME, "serving_model")
DATA_ROOT = tempfile.mkdtemp(prefix="tfx-data")  # Create a temporary directory.

_data_url = "https://raw.githubusercontent.com/tensorflow/tfx/master/tfx/examples/penguin/data/penguins_processed.csv"
_data_filepath = os.path.join(DATA_ROOT, "data.csv")
urllib.request.urlretrieve(_data_url, _data_filepath)


def create_pipeline(
    pipeline_name: str,
    pipeline_root: str,
    data_root: str,
    serving_model_dir: str,
    metadata_path: str
) -> tfx.dsl.Pipeline:
    """
    Creates a three component penguin pipeline with TFX.
    """

    # Brings data into the pipeline.
    example_gen = tfx.components.CsvExampleGen(input_base=data_root)

    # import IPython;IPython.embed()

    # Uses trainer-provided Python function that trains a model.
    trainer = tfx.components.Trainer(
        run_fn="trainer.train.run_fn",
        examples=example_gen.outputs["examples"],
        train_args=tfx.proto.TrainArgs(num_steps=100),
        eval_args=tfx.proto.EvalArgs(num_steps=5),
    )

    # Pushes the model to a filesystem destination.
    pusher = tfx.components.Pusher(
        model=trainer.outputs["model"],
        push_destination=tfx.proto.PushDestination(
            filesystem=tfx.proto.PushDestination.Filesystem(base_directory=serving_model_dir)
        )
    )

    # Following three components will be included in the pipeline.
    components = [example_gen, trainer, pusher]

    p = tfx.dsl.Pipeline(
        pipeline_name=pipeline_name,
        pipeline_root=pipeline_root,
        metadata_connection_config=tfx.orchestration.metadata.sqlite_metadata_connection_config(metadata_path),
        components=components
    )

    return p


if __name__ == "__main__":

    p = create_pipeline(
        pipeline_name=PIPELINE_NAME,
        pipeline_root=PIPELINE_ROOT,
        data_root=DATA_ROOT,
        serving_model_dir=SERVING_MODEL_DIR,
        metadata_path=METADATA_PATH,
    )
    tfx.orchestration.LocalDagRunner().run(p)
