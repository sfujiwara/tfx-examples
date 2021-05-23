import json
import os
from typing import Dict, List
import tensorflow as tf
from tfx import v1 as tfx
from tfx.types.experimental.simple_artifacts import Dataset
from google.cloud import bigquery


@tfx.dsl.components.component
def DataGen(
    data: tfx.dsl.components.OutputArtifact[Dataset],
    query: tfx.dsl.components.Parameter[str] = "",
) -> tfx.dsl.components.OutputDict(hoge=int):

    # data.set_string_custom_property()
    client = bigquery.Client()

    job_config = bigquery.job.QueryJobConfig(
        destination=None,
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE
    )

    query_job = client.query(query, job_config=job_config)

    result: List[Dict] = [dict(row) for row in query_job]

    with tf.io.gfile.GFile(os.path.join(data.uri, "sample.json"), "w") as f:
        f.write(json.dumps(result, indent=2, ensure_ascii=False))

    return {"hoge": 10}


@tfx.dsl.components.component
def Trainer(
    data: tfx.dsl.components.InputArtifact[Dataset],
):
    print("Start training.")
    print(data)


def create_pipeline(
    pipeline_name: str,
    pipeline_root: str,
    metadata_path: str
) -> tfx.dsl.Pipeline:
    """
    Creates a three component penguin pipeline with TFX.
    """

    data_gen_cp = DataGen(
        query="select 1",
    )
    trainer_cp = Trainer(
        data=data_gen_cp.outputs["data"]
    )

    components = [data_gen_cp, trainer_cp]

    p = tfx.dsl.Pipeline(
        pipeline_name=pipeline_name,
        pipeline_root=pipeline_root,
        metadata_connection_config=tfx.orchestration.metadata.sqlite_metadata_connection_config(metadata_path),
        components=components
    )

    return p


if __name__ == "__main__":

    project_id = "sfujiwara"
    pipeline_name = "sample-pipeline"
    pipeline_root = os.path.join("outputs", pipeline_name)
    metadata_path = os.path.join("metadata", pipeline_root, "metadata.db")

    p = create_pipeline(
        pipeline_name=pipeline_name,
        pipeline_root=pipeline_root,
        metadata_path=metadata_path,
    )

    tfx.orchestration.LocalDagRunner().run(p)
