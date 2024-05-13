use diesel::{Connection, r2d2, RunQueryDsl};
use serde::{Deserialize, Serialize};
use crate::DbPool;

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    table_name: String,
    columns: Vec<ColumnSchema>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnSchema {
    name: String,
    data_type: String,
}

pub struct CrudService {
    pool: DbPool,
}

pub enum Error {
    DieselError(diesel::result::Error),
    PoolError(r2d2::Error),
}

impl CrudService {
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool,
        }
    }

    pub async fn create_table(&self, schema: TableSchema) -> Result<(), Error> {
        let table_name = &schema.table_name;
        let columns = &schema.columns;

        let mut conn = self.pool.get().expect("couldn't get db connection from pool");

        match conn.transaction(|| {
            diesel::sql_query(format!("CREATE TABLE {} (", table_name)).execute(&mut conn)?;

            for column in columns {
                diesel::sql_query(format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    table_name, column.name, column.data_type
                ))
                    .execute(&mut conn)?;
            }

            Ok(())
        }) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::DieselError(e)),
        }
    }
}