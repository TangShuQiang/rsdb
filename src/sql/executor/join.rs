use crate::{
    error::{RSDBError, RSDBResult},
    sql::{
        engine::Transaction,
        executor::{Executor, ResultSet},
    },
};

pub struct NestLoopJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
}

impl<T: Transaction> NestLoopJoin<T> {
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { left, right })
    }
}

impl<T: Transaction> Executor<T> for NestLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        // 先执行左边的
        if let ResultSet::Scan {
            columns: left_cols,
            rows: left_rows,
        } = self.left.execute(txn)?
        {
            // 再执行右边的
            if let ResultSet::Scan {
                columns: right_cols,
                rows: right_rows,
            } = self.right.execute(txn)?
            {
                let mut new_rows = Vec::new();
                let mut new_cols = left_cols;
                new_cols.extend(right_cols);
                for lrow in &left_rows {
                    for rrow in &right_rows {
                        let mut row = lrow.clone();
                        row.extend(rrow.clone());
                        new_rows.push(row);
                    }
                }
                return Ok(ResultSet::Scan {
                    columns: new_cols,
                    rows: new_rows,
                });
            }
        }
        Err(RSDBError::Internal(
            "Failed to execute nested loop join".to_string(),
        ))
    }
}
