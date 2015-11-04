__all__ = ["run"]

import importlib
import logging
import os
import re
import sys
from datetime import datetime
from glob import glob

from peewee import *

logger = logging.getLogger(__name__)

INIT_VERSION = 0

class MigrationError(Exception):
    pass


def generate_model(db):
    class Migration(Model):
        """This model it self can't be migrated, so don't change it's structure unless necessary."""
        version = IntegerField(primary_key=True)
        latest_migrate = DateTimeField(null=False)

        class Meta:
            database = db

    return Migration


def get_versions(migration_dir):
    migrate_files = glob(os.path.join(migration_dir, "ver_[0-9]*.py"))

    # Put the version INIT_VERSION into version list.
    # It represent the initial version of the data structure, and there's not a ver_xxx.py file for it.
    versions = [INIT_VERSION]
    for name in migrate_files:
        match = re.search(r"ver_(\d+)\.py$", name).groups()[0]
        versions.append(int(match))
    versions.sort()

    return versions


def execute_migrate_code(migration_dir, module_name, db):
    sys.path.insert(0, os.path.abspath(migration_dir))

    module = importlib.import_module(module_name)
    module.run(db)

    sys.path = sys.path[1:]


def run(db, migration_dir):
    Migration = generate_model(db)
    if not Migration.table_exists():
        if os.path.exists(os.path.join(migration_dir, "initialize.py")):
            with db.transaction():
                execute_migrate_code(migration_dir, "initialize", db)

                db.create_tables([Migration], safe=True)
                Migration.create(version=INIT_VERSION,
                                 latest_migrate=datetime.now())

            logger.info("initialize complete, version {}.".format(INIT_VERSION))
        else:
            raise MigrationError("initialize.py not found")

    versions = get_versions(migration_dir)

    if Migration.table_exists():
        current_version = Migration.select().get().version

        if current_version not in versions:
            raise MigrationError("version '{}' not found in local".format(current_version))
        elif current_version == versions[-1]:
            logger.debug("Already latest version {}, doesn't need migrate.".format(current_version))
        else:
            with db.transaction():
                for version in versions:
                    if version > current_version:
                        module_name = "ver_{}".format(version)
                        execute_migrate_code(migration_dir, module_name, db)

                query = Migration.update(version=versions[-1], latest_migrate=datetime.now())
                query.execute()

                logger.info("from version {} to {}, migrate complete.".format(current_version, versions[-1]))