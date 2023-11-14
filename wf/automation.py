import uuid

from latch.account import Account
from latch.resources.tasks import small_task
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile
from wf.processing import processing_workflow


@small_task
def automation_task(
    input_directory: LatchDir, output_directory: LatchOutputDir, automation_id: str
) -> None:
    account = Account.current()
    automation_project_display_name = "Automations"

    automation_project = next(
        (
            project
            for project in account.list_registry_projects()
            if project.get_display_name() == automation_project_display_name
        ),
        None,
    )
    if not automation_project:
        with account.update() as account_updater:
            account_updater.upsert_registry_project(
                display_name=automation_project_display_name
            )
        automation_project = next(
            project
            for project in account.list_registry_projects()
            if project.get_display_name() == automation_project_display_name
        )

    automation_table = next(
        (
            table
            for table in automation_project.list_tables()
            if table.get_display_name() == f"automation-{automation_id}"
        ),
        None,
    )
    if not automation_table:
        with automation_project.update() as automation_project_updater:
            automation_project_updater.upsert_table(
                display_name=f"automation-{automation_id}"
            )
        automation_table = next(
            table
            for table in automation_project.list_tables()
            if table.get_display_name() == f"automation-{automation_id}"
        )

    if not automation_table.get_columns().get("Resolved directories", None):
        with automation_table.update() as automation_table_updater:
            automation_table_updater.upsert_column("Resolved directories", LatchDir)

    resolved_directories = set()
    for page in automation_table.list_records():
        for _, record in page.items():
            resolved_directories.add(str(record.get_values()["Resolved directories"]))

    for dir in input_directory.iterdir():
        if (
            type(dir) == LatchFile
            or str(dir) == str(output_directory)
            or str(dir) in resolved_directories
        ):
            continue

        with automation_table.update() as automation_table_updater:
            processing_workflow(input_directory=dir, output_directory=output_directory)
            automation_table_updater.upsert_record(
                f"{str(uuid.uuid4())[-8:]}",
                **{
                    "Resolved directories": dir,
                },
            )
