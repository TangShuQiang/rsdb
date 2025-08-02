use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    sql::{
        engine::{Engine, Transaction},
        parser::ast::Expression,
        schema::Table,
        types::{Row, Value},
    },
    storage::{self, engine::Engine as StorageEngine, keycode::serialize_key},
};

// KV Engin 定义
pub struct KVEngine<E: StorageEngine> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self {
            kv: self.kv.clone(),
        }
    }
}

impl<E: StorageEngine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

// KV Transaction 定义，实际上对存储引擎中 MvccTransaction 的封装
pub struct KVTransaction<E: StorageEngine> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(&self) -> Result<()> {
        self.txn.rollback()
    }

    fn create_row(&self, table: &Table, row: Row) -> Result<()> {
        // 校验行的有效性
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => continue,
                None => {
                    return Err(Error::Internal(format!(
                        "column {} cannot be null",
                        col.name
                    )));
                }
                Some(dt) if dt != col.datatype => {
                    return Err(Error::Internal(format!(
                        "column {} type mismatch",
                        col.name
                    )));
                }
                _ => continue,
            }
        }
        // 找到表中的主键作为一行数据的唯一标识
        let pk = table.get_primary_key(&row)?;
        // 查看主键对应的数据是否已经存在了
        let id = Key::Row(table.name.clone(), pk.clone()).encode()?;
        if self.txn.get(id.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "Duplicate data for primary key {:?} in table {}",
                pk,
                table.name.clone()
            )));
        }
        // 存放数据
        let value = bincode::serialize(&row)?;
        self.txn.set(id, value)
    }

    fn update_row(&self, table: &Table, old_pk: &Value, row: Row) -> Result<()> {
        let new_pk = table.get_primary_key(&row)?;
        // 更新了主键，则删除旧的数据
        if *old_pk != new_pk {
            let key = Key::Row(table.name.clone(), old_pk.clone()).encode()?;
            self.txn.delete(key)?;
        }
        let key = Key::Row(table.name.clone(), new_pk).encode()?;
        let value = bincode::serialize(&row)?;
        self.txn.set(key, value)
    }

    fn delete_row(&self, table: &Table, pk: &Value) -> Result<()> {
        let key = Key::Row(table.name.clone(), pk.clone()).encode()?;
        self.txn.delete(key)
    }

    fn scan_table(&self, table: &Table, filter: Option<(String, Expression)>) -> Result<Vec<Row>> {
        let prefix = KeyPrefix::Row(table.name.clone()).encode()?;
        let results = self.txn.scan_prefix(prefix)?;
        let mut rows = Vec::new();
        for result in results {
            // 过滤数据
            let row: Row = bincode::deserialize(&result.value)?;
            if let Some((col, expr)) = &filter {
                let col_index = table.get_col_index(&col)?;
                if Value::from_expression(expr.clone()) == row[col_index] {
                    rows.push(row);
                }
            } else {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn create_table(&self, table: Table) -> Result<()> {
        // 判断表是否存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} already exists",
                table.name
            )));
        }
        // 判断表的有效性
        table.validate()?;
        let key = Key::Table(table.name.clone()).encode()?;
        let value = bincode::serialize(&table)?;
        self.txn.set(key, value)
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name).encode()?;
        Ok(self
            .txn
            .get(key)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),
    Row(String, Value),
}

impl Key {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[derive(Debug, Serialize)]
enum KeyPrefix {
    Table,
    Row(String),
}

impl KeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        sql::engine::{Engine, kv::KVEngine},
        storage::memory::MemoryEngine,
    };

    #[test]
    fn test() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let s = kvengine.session()?;

        s.execute(
            "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
        )?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        let v = s.execute("update t1 set b = 'aa' where a = 1;")?;
        println!("{:?}", v);
        let v = s.execute("update t1 set a = 33 where a = 3;")?;
        println!("{:?}", v);

        let v = s.execute("delete from t1 where a = 2;")?;
        println!("{:?}", v);

        match s.execute("select * from t1;")? {
            crate::sql::executor::ResultSet::Scan { columns, rows } => {
                for row in rows {
                    println!("{:?} ", row);
                }
            }
            _ => panic!("Expected a scan result"),
        }

        Ok(())
    }
}
