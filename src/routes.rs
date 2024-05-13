use actix_web::{get, HttpResponse, post, Responder, web};
use crate::services::crud::{CrudService, Error, TableSchema};

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

#[post("/tables")]
async fn create_table(schema: web::Json<TableSchema>, service: web::Data<CrudService>) -> impl Responder {
    let table_schema = schema.into_inner();
    match service.create_table(table_schema).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(Error::DieselError(e)) => HttpResponse::InternalServerError().body(format!("Diesel error: {}", e)),
        Err(Error::PoolError(e)) => HttpResponse::InternalServerError().body(format!("Pool error: {}", e)),
    }
}
