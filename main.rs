use std::collections::HashMap;
use sqlx::{Column, Pool, Row, Sqlite};
use sqlx::sqlite::{SqliteColumn, SqlitePool, SqliteRow};
use polars::prelude::*;

#[async_std::main]
async fn main() -> Result<(), sqlx::Error> {
    let path_to_db = ".\\Cohort.sqlite";

    let mut pool = SqlitePool::connect(path_to_db).await?;
    let sql_cmd = "SELECT * FROM cohort WHERE chart_number = '6266286' ";
    // let sql_cmd = "SELECT * FROM cohort";
    let mut rows = sqlx::query(sql_cmd).fetch_all(&pool).await?;
    let df = sqlx_to_polars(sql_cmd, &pool).await;
    println!("{}", df);
    Ok(())

}

async fn sqlx_to_polars(sql_cmd: &str, conn: &Pool<Sqlite>) -> DataFrame{
    let mut map_of_cols = Vec::<Series>::new();
    let first_row = sqlx::query(sql_cmd).fetch_one(conn).await;
    let mut cols_list = Vec::<String>::new();
    if let Ok(first_row) = first_row{
        cols_list = first_row.columns().into_iter().map(|x|x.name().to_owned()).collect::<Vec<String>>();;
    }else {
        panic!("read sql error");
    }
    println!("now cols = {:?} ", cols_list);

    for col_name in cols_list {
        println!("getting col_name {:?}", col_name);
        // let sql_cmd = format!("SELECT CAST([{}] AS TEXT) as [{}] FROM cohort "
        //                       ,col_name, col_name);
        let sql_cmd = format!("SELECT CAST([{}] AS TEXT) as [{}] FROM ({}) "
                              ,col_name, col_name, sql_cmd);
        let col_data = sqlx::query(&sql_cmd).fetch_all(conn).await.unwrap();

        let mut col_data_to_list: Vec<Option<String>> = Vec::new();
        for i in col_data{
            let j = i.try_get::<&str, &str>(col_name.as_str());
            if let Ok(j) = j{
                col_data_to_list.push(Some(j.to_owned()));
            }else {
                col_data_to_list.push(None);
            }
        }
        // println!("{:?}", col_data_to_list);
        map_of_cols.push(Series::new(&col_name, col_data_to_list));
    }
    println!("{:?}", map_of_cols);
    DataFrame::new(map_of_cols).unwrap()
}
