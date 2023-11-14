from latch.resources.tasks import small_task
from latch.resources.workflow import workflow
from latch.types.directory import LatchDir, LatchOutputDir


@small_task
def processing_task(
    input_directory: LatchDir, output_directory: LatchOutputDir
) -> None:
    print(f"Starting workflow on directory: {input_directory}")
    # Processing Code here


@workflow
def processing_workflow(
    input_directory: LatchDir, output_directory: LatchOutputDir
) -> None:
    processing_task(
        input_directory=input_directory,
        output_directory=output_directory,
    )
