use crate::{
    error::{RSDBError, RSDBResult},
    sql::{
        executor::ResultSet,
        parser::{Parser, ast::Expression},
        plan::Plan,
        schema::Table,
        types::{Row, Value},
    },
};

mod kv;

// 抽象的 SQL 引擎层定义，目前只有一个 KVEngine
pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> RSDBResult<Self::Transaction>;

    fn session(&self) -> RSDBResult<Session<Self>> {
        Ok(Session {
            engin: self.clone(),
        })
    }
}

// 抽象的事务信息，包含了 DDL 和 DML 操作
// 底层可以接入普通的 KV 存储引擎，也可以接入分布式存储引擎
pub trait Transaction {
    // 提交事务
    fn commit(&self) -> RSDBResult<()>;
    // 回滚事务
    fn rollback(&self) -> RSDBResult<()>;

    // 创建行
    fn create_row(&self, table: &Table, row: Row) -> RSDBResult<()>;
    // 更新行
    fn update_row(&self, table: &Table, old_pk: &Value, row: Row) -> RSDBResult<()>;
    // 删除行
    fn delete_row(&self, table: &Table, pk: &Value) -> RSDBResult<()>;
    // 扫描表
    fn scan_table(&self, table: &Table, filter: Option<(String, Expression)>) -> RSDBResult<Vec<Row>>;

    // DDL 相关操作
    fn create_table(&self, table: Table) -> RSDBResult<()>;
    // 获取表信息
    fn get_table(&self, table_name: String) -> RSDBResult<Option<Table>>;
    // 获取表信息，若不存在则报错
    fn must_get_table(&self, table_name: String) -> RSDBResult<Table> {
        self.get_table(table_name.clone())?
            .ok_or(RSDBError::Internal(format!(
                "table {} does not exist",
                table_name
            )))
    }
}

// 客户端 session 定义
pub struct Session<E: Engine> {
    engin: E,
}

impl<E: Engine + 'static> Session<E> {
    // 执行客户端 SQL 语句
    pub fn execute(&self, sql: &str) -> RSDBResult<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engin.begin()?;
                // 构建 plan，执行 SQL 语句
                match Plan::build(stmt).execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    }
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }
}
