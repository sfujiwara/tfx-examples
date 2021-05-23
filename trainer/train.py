from tfx import v1 as tfx
from . import util


def run_fn(fn_args: tfx.components.FnArgs):

    print("hello")
    util.save_model(fn_args.serving_model_dir)
