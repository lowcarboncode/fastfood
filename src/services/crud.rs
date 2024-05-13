use diesel::{Connection, r2d2, RunQueryDsl};
use serde::{Deserialize, Serialize};
use log::log;
use crate::DbPool;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DataType {
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "integer")]
    Integer,
    #[serde(rename = "float")]
    Float,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "timestamp")]
    TimeStamp,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataType::Text => write!(f, "TEXT"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "REAL"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::TimeStamp => write!(f, "TIMESTAMP"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    pub primary_key: Option<bool>,
    pub auto_increment: Option<bool>,
    pub unique: Option<bool>,
    pub not_null: Option<bool>,
    pub default: Option<String>,
}

impl std::fmt::Display for ColumnSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {}{}{}{}{}{}",
               self.name,
               self.data_type.to_string().to_uppercase(),
               if self.primary_key.unwrap_or(false) { " PRIMARY KEY" } else { "" },
               if self.auto_increment.unwrap_or(false) { " AUTOINCREMENT" } else { "" },
               if self.unique.unwrap_or(false) { " UNIQUE" } else { "" },
               if self.not_null.unwrap_or(false) { " NOT NULL" } else { "" },
               if let Some(ref default) = self.default {
                   format!(" DEFAULT {}", default)
               } else {
                   "".to_string()
               }
        )
    }
}

pub struct CrudService {
    pool: DbPool,
    id_col: ColumnSchema,
    created_at_col: ColumnSchema,
    updated_at_col: ColumnSchema,
}

pub enum Error {
    DieselError(diesel::result::Error),
    PoolError(r2d2::Error),
}

impl CrudService {
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool,
            id_col: ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Integer,
                primary_key: Some(true),
                auto_increment: Some(true),
                unique: Some(true),
                not_null: Some(true),
                default: None,
            },
            created_at_col: ColumnSchema {
                name: "created_at".to_string(),
                data_type: DataType::TimeStamp,
                primary_key: Some(false),
                auto_increment: Some(false),
                unique: Some(false),
                not_null: Some(true),
                default: Some("CURRENT_TIMESTAMP".to_string()),
            },
            updated_at_col: ColumnSchema {
                name: "updated_at".to_string(),
                data_type: DataType::TimeStamp,
                primary_key: Some(false),
                auto_increment: Some(false),
                unique: Some(false),
                not_null: Some(true),
                default: Some("CURRENT_TIMESTAMP".to_string()),
            },
        }
    }

    pub async fn create_table(&self, schema: TableSchema) -> Result<TableSchema, Error> {
        let table_name = &schema.name;
        let columns = &schema.columns;

        let mut conn = self.pool.get().expect("couldn't get db connection from pool");

        match conn.transaction(|conn| {
            let mut query_columns = format!("{}", self.id_col);

            for column in columns {
                query_columns.push_str(&format!(", {}", column));
            }

            query_columns.push_str(&format!(", {}", self.created_at_col));
            query_columns.push_str(&format!(", {}", self.updated_at_col));

            let create_query = format!("CREATE TABLE {} ({})", table_name, query_columns);

            log::info!("Executing query: {}", create_query);

            diesel::sql_query(create_query).execute(conn)?;

            // create trigger for updated_at
            let trigger_query = format!("CREATE TRIGGER update_{table_name}_updated_at
                AFTER UPDATE ON {table_name}
                FOR EACH ROW
                BEGIN
                    UPDATE {table_name} SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
                END;", table_name = table_name);

            log::info!("Executing query: {}", trigger_query);
            diesel::sql_query(trigger_query).execute(conn)?;

            Ok(())
        }) {
            Ok(_) => {
                let mut cols = vec![self.id_col.clone()];
                cols.extend(columns.iter().cloned());
                cols.push(self.created_at_col.clone());
                cols.push(self.updated_at_col.clone());

                Ok(TableSchema {
                    name: table_name.to_string(),
                    columns: cols,
                })
            }
            Err(e) => {
                log::error!("Error creating table: {}", e);
                Err(Error::DieselError(e))
            }
        }
    }

    pub async fn drop_table(&self, table_name: &str) -> Result<(), Error> {
        let mut conn = self.pool.get().expect("couldn't get db connection from pool");

        let drop_query = format!("DROP TABLE {}", table_name);

        log::info!("Executing query: {}", drop_query);

        match diesel::sql_query(drop_query).execute(&mut conn) {
            Ok(_) => Ok(()),
            Err(e) => {
                log::error!("Error dropping table: {}", e);
                return Err(Error::DieselError(e));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::sqlite::SqliteConnection;
    use diesel::r2d2::ConnectionManager;
    use diesel::prelude::*;

    fn get_pool() -> DbPool {
        dotenv::dotenv().ok();
        let database_url = "test.sqlite";
        let manager = ConnectionManager::<SqliteConnection>::new(database_url);
        r2d2::Pool::builder()
            .build(manager)
            .expect("Failed to create pool.")
    }

    #[actix_web::test]
    async fn test_create_table() {
        let pool = get_pool();
        let service = CrudService::new(pool);

        let schema = TableSchema {
            name: "test_table".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    primary_key: Some(false),
                    auto_increment: Some(false),
                    unique: Some(false),
                    not_null: Some(true),
                    default: None,
                },
                ColumnSchema {
                    name: "age".to_string(),
                    data_type: DataType::Integer,
                    primary_key: Some(false),
                    auto_increment: Some(false),
                    unique: Some(false),
                    not_null: Some(true),
                    default: None,
                },
            ],
        };

        let result = service.create_table(schema).await;
        assert!(result.is_ok());
    }

    #[actix_web::test]
    async fn test_drop_table() {
        let pool = get_pool();
        let service = CrudService::new(pool);

        let result = service.drop_table("test_table").await;
        assert!(result.is_ok());
    }
}