from pathlib import Path
import uuid
import csv

from latch.resources.tasks import small_task
from latch.types.directory import LatchOutputDir, LatchDir
from latch.account import Account


@small_task
def task(
    input_directory: LatchDir, output_directory: LatchOutputDir, automation_id: str
) -> None:
    account = Account.current()
    automation_project_display_name = "Automations"

    with account.update() as account_updater:
        account_updater.upsert_registry_project(
            display_name=automation_project_display_name
        )
    automation_project = next(
        project
        for project in account.list_registry_projects()
        if project.get_display_name() == automation_project_display_name
    )

    with automation_project.update() as automation_project_updater:
        automation_project_updater.upsert_table(
            display_name=f"automation-{automation_id}"
        )
    automation_table = next(
        table
        for table in automation_project.list_tables()
        if table.get_display_name() == f"automation-{automation_id}"
    )

    with automation_table.update() as automation_table_updater:
        automation_table_updater.upsert_column("Resolved directories", LatchDir)

    resolved_directories = set()
    for page in automation_table.list_records():
        for _, record in page.items():
            resolved_directories.add(str(record.get_values()["Resolved directories"]))

    for dir in input_directory.iterdir():
        if (
            type(dir) == LatchDir
            and dir != output_directory
            and str(dir) in resolved_directories
        ):
            continue

        print(f"Starting workflow on directory: {dir}")

        with automation_table.update() as automation_table_updater:
            automation_table_updater.upsert_record(
                f"{str(uuid.uuid4())[-8:]}",
                **{
                    "Resolved directories": dir,
                },
            )
