use std::collections::HashSet;

use crate::{
    error::{RSDBError, RSDBResult},
    sql::{
        executor::ResultSet,
        parser::{
            Parser,
            ast::{self, Expression},
        },
        plan::Plan,
        schema::Table,
        types::{Row, Value},
    },
};

pub mod kv;

// 抽象的 SQL 引擎层定义，目前只有一个 KVEngine
pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> RSDBResult<Self::Transaction>;

    fn session(&self) -> RSDBResult<Session<Self>> {
        Ok(Session {
            engin: self.clone(),
            txn: None,
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
    // 版本号
    fn version(&self) -> u64;

    // 创建行
    fn create_row(&self, table: &Table, row: Row) -> RSDBResult<()>;
    // 更新行
    fn update_row(&self, table: &Table, old_pk: &Value, row: Row) -> RSDBResult<()>;
    // 删除行
    fn delete_row(&self, table: &Table, pk: &Value) -> RSDBResult<()>;
    // 扫描表
    fn scan_table(&self, table: &Table, filter: Option<Expression>) -> RSDBResult<Vec<Row>>;

    // 获取索引
    fn load_index(
        &self,
        table_name: &str,
        col_name: &str,
        col_value: &Value,
    ) -> RSDBResult<HashSet<Value>>;
    // 保存索引
    fn save_index(
        &self,
        table_name: &str,
        col_name: &str,
        col_value: &Value,
        index: HashSet<Value>,
    ) -> RSDBResult<()>;
    // 根据主键获取行
    fn read_by_pk(&self, table_name: &str, pk: &Value) -> RSDBResult<Option<Row>>;

    // DDL 相关操作
    // 创建表
    fn create_table(&self, table: Table) -> RSDBResult<()>;
    // 删除表
    fn drop_table(&self, table_name: String) -> RSDBResult<()>;
    // 获取所有的表名
    fn get_table_names(&self) -> RSDBResult<Vec<String>>;
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
    txn: Option<E::Transaction>,
}

impl<E: Engine + 'static> Session<E> {
    // 执行客户端 SQL 语句
    pub fn execute(&mut self, sql: &str) -> RSDBResult<ResultSet> {
        match Parser::new(sql).parse()? {
            ast::Statement::Begin if self.txn.is_some() => {
                Err(RSDBError::Internal("Already in transaction".to_string()))
            }
            ast::Statement::Commit | ast::Statement::Rollback if self.txn.is_none() => {
                Err(RSDBError::Internal("Not in transaction".to_string()))
            }
            ast::Statement::Begin => {
                let txn = self.engin.begin()?;
                let version = txn.version();
                self.txn = Some(txn);
                Ok(ResultSet::Begin { version })
            }
            ast::Statement::Commit => {
                let txn = self.txn.take().unwrap();
                let version = txn.version();
                txn.commit()?;
                Ok(ResultSet::Commit { version })
            }
            ast::Statement::Rollback => {
                let txn = self.txn.take().unwrap();
                let version = txn.version();
                txn.rollback()?;
                Ok(ResultSet::Rollback { version })
            }
            stmt if self.txn.is_some() => {
                Plan::build(stmt, self.txn.as_mut().unwrap())?.execute(self.txn.as_mut().unwrap())
            }
            stmt => {
                let mut txn = self.engin.begin()?;
                // 构建 plan，执行 SQL 语句
                match Plan::build(stmt, &mut txn)?.execute(&mut txn) {
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

    pub fn get_table(&self, table_name: String) -> RSDBResult<String> {
        let table = match self.txn.as_ref() {
            Some(txn) => txn.must_get_table(table_name)?,
            None => {
                let txn = self.engin.begin()?;
                let table = txn.must_get_table(table_name)?;
                txn.commit()?;
                table
            }
        };
        Ok(table.to_string())
    }

    pub fn get_table_names(&self) -> RSDBResult<String> {
        let names = match self.txn.as_ref() {
            Some(txn) => txn.get_table_names()?,
            None => {
                let txn = self.engin.begin()?;
                let names = txn.get_table_names()?;
                txn.commit()?;
                names
            }
        };
        Ok(names.join("\n"))
    }
}
