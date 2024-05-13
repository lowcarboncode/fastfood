mod routes;
mod services;

use actix_web::{web, App, HttpServer, Responder};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use dotenv::dotenv;
use std::sync::{Arc, Mutex};
use diesel::r2d2;
use serde::{Deserialize, Serialize};

struct AppState {
    db: Arc<Mutex<SqliteConnection>>,
}

pub type DbPool = r2d2::Pool<r2d2::ConnectionManager<SqliteConnection>>;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let manager = r2d2::ConnectionManager::<SqliteConnection>::new("app.sqlite");
    let pool = r2d2::Pool::builder()
        .build(manager)
        .expect("database URL should be valid path to SQLite DB file");

    let crud_service = Arc::new(services::crud::CrudService::new(pool.clone()));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::from(crud_service.clone()))
            .service(routes::create_table)
            .service(routes::health)
    })
        .bind("0.0.0.0:8080")?
        .run()
        .await
}
