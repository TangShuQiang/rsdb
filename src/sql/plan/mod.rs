use crate::sql::{
    parser::ast::{self, Expression}, plan::planner::Planner, schema::Table
};

mod planner;

// 执行节点
#[derive(Debug, PartialEq)]
pub enum Node {
    // 创建表
    CreateTable {
        schema: Table,
    },

    // 插入数据
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },

    // 扫描节点
    Scan {
        table_name: String,
    },
}

// 执行计划定义，底层是不同类型执行节点
#[derive(Debug, PartialEq)]
pub struct Plan(pub Node);

impl Plan {
    pub fn build(stmt: ast::Statement) -> Self {
        Planner::new().build(stmt)
    }
}
