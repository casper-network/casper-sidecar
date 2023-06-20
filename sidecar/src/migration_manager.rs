use crate::types::database::{
    DatabaseReadError, DatabaseReader, DatabaseWriteError, DatabaseWriter, Migration,
};
use anyhow::Error;
use itertools::Itertools;
use std::collections::HashMap;

#[cfg(test)]
mod tests;

pub struct MigrationManager<T>
where
    T: DatabaseWriter + DatabaseReader + Send + Sync + 'static,
{
    db: T,
}

impl<T> MigrationManager<T>
where
    T: DatabaseWriter + DatabaseReader + Send + Sync + 'static,
{
    pub async fn migrate(&self, migrations: Vec<Migration>) -> Result<(), Error> {
        self.db
            .execute_migration(Migration::initial())
            .await
            .map_err(error_when_executing_initial_migration)?;
        // all the subsequent migrations should have version
        if migrations
            .clone()
            .into_iter()
            .any(|m| m.get_version().is_none())
        {
            return Err(error_migration_without_version());
        }
        validate_uniqueness_of_migration_versions(migrations.clone())?;
        let maybe_newest_version = self
            .db
            .get_newest_migration_version()
            .await
            .map_err(error_version_fetch_failed)?;
        self.do_migrate(migrations, maybe_newest_version).await
    }

    pub async fn apply_all_migrations(db: T) -> Result<(), Error> {
        let m = MigrationManager { db };
        m.migrate(Migration::get_all_migrations()).await
    }

    async fn do_migrate(
        &self,
        migrations: Vec<Migration>,
        maybe_newest_version: Option<(u32, bool)>,
    ) -> Result<(), Error> {
        if migrations.is_empty() {
            return Ok(());
        }
        let migrations_to_execute = if let Some((v, success)) = maybe_newest_version {
            if !success {
                return Err(error_last_migration_failed(v));
            }
            // Migrations with version "None" are initial scaffolding migrations and should only be executed
            // if no other migrations were yet executed.
            migrations
                .into_iter()
                .filter(|el| el.get_version().is_some() && el.get_version().unwrap() > v)
                .sorted_by_key(|x| x.get_version())
                .collect::<Vec<Migration>>()
        } else {
            migrations
                .into_iter()
                .sorted_by_key(|x| x.get_version())
                .collect::<Vec<Migration>>()
        };

        for migration in migrations_to_execute {
            let version = migration.get_version().unwrap(); //at this point migrations should have versions
            self.db
                .execute_migration(migration)
                .await
                .map_err(|err| error_execute_migration_failed(version, err))?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn apply_migrations(db: T, migrations: Vec<Migration>) -> Result<(), Error> {
        let m = MigrationManager { db };
        m.migrate(migrations).await
    }
}

fn error_migration_without_version() -> Error {
    Error::msg("Found a migration without version set! Migration passed to #migrate should all have versions!")
}

fn error_when_executing_initial_migration(err: DatabaseWriteError) -> Error {
    Error::msg(format!("Error when executing initial migration {:?}", err))
}

fn error_last_migration_failed(version: u32) -> Error {
    Error::msg(format!(
        "Cannot proceed with migration, newest migration version: {} didn't finish correctly.",
        version
    ))
}

fn error_execute_migration_failed(version: u32, err: DatabaseWriteError) -> Error {
    Error::msg(format!(
        "Error when executing migration {:?}. Underlying: {:?}",
        version, err
    ))
}

fn error_version_fetch_failed(err: DatabaseReadError) -> Error {
    Error::msg(format!(
        "Error when fetching newest migration version: {:?}",
        err
    ))
}

fn validate_uniqueness_of_migration_versions(migrations: Vec<Migration>) -> Result<(), Error> {
    let mut versions = HashMap::new();
    let first_duplicate_version = migrations.into_iter().find_map(|x| {
        if let Some(version) = x.get_version() {
            if let std::collections::hash_map::Entry::Vacant(e) = versions.entry(version) {
                e.insert(true);
                None
            } else {
                Some(version)
            }
        } else {
            None
        }
    });
    if let Some(version) = first_duplicate_version {
        Err(Error::msg(format!(
            "Duplicate version {} defined in migrations.",
            version
        )))
    } else {
        Ok(())
    }
}
