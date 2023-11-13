from argparse import ArgumentDefaultsHelpFormatter
from re import I
from wf.task import task

from latch.resources.workflow import workflow
from latch.types.directory import LatchOutputDir, LatchDir
from latch.types.file import LatchFile
from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter

metadata = LatchMetadata(
    display_name="Automation Template",
    author=LatchAuthor(
        name="Ryan Teoh",
    ),
    parameters={
        "input_directory": LatchParameter(
            display_name="Input Directory",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "output_directory": LatchParameter(
            display_name="Output Directory",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
        "automation_id": LatchParameter(
            display_name="Automation ID",
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
    },
)


@workflow(metadata)
def template_workflow(
    input_directory: LatchDir, output_directory: LatchOutputDir, automation_id: str
) -> None:
    task(
        input_directory=input_directory,
        output_directory=output_directory,
        automation_id=automation_id,
    )
