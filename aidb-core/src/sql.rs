use eyre::{Result, eyre};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1, none_of, one_of},
    combinator::{eof, fail, map, map_opt, map_res, opt, recognize, value},
    error::ParseError,
    multi::{fold_many0, many0, many0_count, many1, separated_list1},
    number::complete::hex_u32,
    sequence::{delimited, preceded, separated_pair, terminated},
};
use nom_language::precedence::{Assoc, Operation, binary_op, precedence, unary_op};
use tracing::trace;

use crate::{Aidb, Column, DataType, Value};

#[derive(Debug, Clone)]
pub enum SqlStmt {
    /// SHOW TABLES
    ShowTables,
    /// DESCRIBE | DESC table
    Describe { table: String },
    /// CREATE TABLE table (column datatype, ...)
    CreateTable { table: String, columns: Vec<Column> },
    /// INSERT INTO table [(column, ...)] VALUES value, ...
    InsertInto {
        table: String,
        columns: Vec<SqlCol>,
        values: Vec<Vec<Value>>,
    },
    /// SELECT column, ... [FROM table] [JOIN table ON condition ...] [WHERE condition]
    Select {
        columns: Vec<SqlSelectTarget>,
        table: Option<String>,
        join_on: Vec<(String, SqlOn)>,
        where_: Option<SqlWhere>,
        limit: Option<u64>,
    },
    /// UPDATE table SET column = value, ... [WHERE condition]
    Update {
        table: String,
        set: Vec<(SqlCol, Value)>,
        where_: Option<SqlWhere>,
    },
    /// DELETE FROM table [WHERE condition]
    DeleteFrom {
        table: String,
        where_: Option<SqlWhere>,
    },
}

#[derive(Debug, Clone)]
pub enum SqlCol {
    /// implicit table name
    Short(String),
    /// table.column
    Full { table: String, column: String },
}

#[derive(Debug, Clone)]
pub struct SqlOn {
    pub lhs: SqlCol,
    pub rhs: SqlCol,
}

#[derive(Debug, Clone)]
pub enum SqlSelectTarget {
    Column(SqlCol),
    Const(Value),
    Wildcard,
    Variable(String),
}

#[derive(Debug, Clone)]
pub enum SqlColOrExpr {
    Column(SqlCol),
    Const(Value),
}

#[derive(Debug, Clone)]
pub enum SqlRel {
    Eq {
        lhs: SqlColOrExpr,
        rhs: SqlColOrExpr,
    },
    Le {
        lhs: SqlColOrExpr,
        rhs: SqlColOrExpr,
    },
    Like {
        lhs: SqlCol,
        rhs: String,
    },
}

#[derive(Debug, Clone)]
pub enum SqlWhere {
    Rel(SqlRel),
    And(Box<SqlWhere>, Box<SqlWhere>),
    Or(Box<SqlWhere>, Box<SqlWhere>),
    Not(Box<SqlWhere>),
}

impl Aidb {
    pub fn complete(input: impl AsRef<str>) -> String {
        for (tail, hint) in [
            ("SELECT 1", "SELECT"),
            ("FROM a", "FROM"),
            ("ON a = a", "ON"),
            ("WHERE a = a", "WHERE"),
            ("= a", "="),
            ("LIKE \"\"", "LIKE"),
            ("LIMIT 1", "LIMIT"),
            ("INTO a(a) VALUES (1)", "INTO"),
            ("VALUES (1)", "VALUES"),
            ("TABLE a (a INTEGER)", "TABLE"),
            (")", ")"),
            (";", ";"),
        ] {
            if stmt(&format!("{} {tail}", input.as_ref())).is_ok() {
                return hint.to_owned();
            }
        }
        "".to_owned()
    }

    pub(crate) fn parse(input: impl AsRef<str>) -> Result<SqlStmt> {
        match stmt(input.as_ref()) {
            Ok((remain, stmt)) => {
                assert!(remain.is_empty());
                Ok(stmt)
            }
            Err(e) => match e {
                nom::Err::Error(e) => {
                    trace!(?e);
                    Err(eyre!("SQL invalid"))
                }
                _ => unreachable!(),
            },
        }
    }
}

fn kw_preceded<'a, 'b, E: ParseError<&'a str>>(
    kw: &'b str,
) -> impl Parser<&'a str, Output = &'a str, Error = E> {
    delimited(multispace0, tag_no_case(kw), multispace1)
}

fn kw<'a, 'b, E: ParseError<&'a str>>(
    kw: &'b str,
) -> impl Parser<&'a str, Output = &'a str, Error = E> {
    delimited(multispace1, tag_no_case(kw), multispace1)
}

fn comma_list1<'a, T, E: ParseError<&'a str>>(
    parser: impl Parser<&'a str, Output = T, Error = E>,
) -> impl Parser<&'a str, Output = Vec<T>, Error = E> {
    separated_list1((multispace0, tag(","), multispace0), parser)
}

fn paren<'a, T, E: ParseError<&'a str>>(
    parser: impl Parser<&'a str, Output = T, Error = E>,
) -> impl Parser<&'a str, Output = T, Error = E> {
    delimited((tag("("), multispace0), parser, (multispace0, tag(")")))
}

type ParseResult<'a, T> = IResult<&'a str, T>;

fn ident(input: &str) -> ParseResult<String> {
    map(
        recognize((
            alt((alpha1, tag("_"))),
            many0_count(alt((alphanumeric1, tag("_")))),
        )),
        |ident: &str| ident.to_owned(),
    )
    .parse(input)
}

fn col(input: &str) -> ParseResult<SqlCol> {
    alt((
        map(separated_pair(ident, tag("."), ident), |(table, column)| {
            SqlCol::Full { table, column }
        }),
        map(ident, SqlCol::Short),
    ))
    .parse(input)
}

fn stmt(input: &str) -> ParseResult<SqlStmt> {
    delimited(
        multispace0,
        alt((show_tables, describe, create_table, insert_into, select)),
        (multispace0, opt(tag(";")), multispace0, eof),
    )
    .parse(input)
}

fn datatype(input: &str) -> ParseResult<DataType> {
    use DataType::*;
    alt((
        value(
            Integer,
            alt((
                tag_no_case("INTEGER"),
                tag_no_case("INT"),
                tag_no_case("BIGINT"),
                tag_no_case("SMALLINT"),
            )),
        ),
        value(
            Real,
            alt((
                tag_no_case("REAL"),
                tag_no_case("FLOAT"),
                tag_no_case("DOUBLE"),
            )),
        ),
        alt((
            value(Text, tag_no_case("TEXT")),
            value(
                Text,
                (
                    opt(tag_no_case("VAR")),
                    tag_no_case("CHAR"),
                    multispace0,
                    tag("("),
                    multispace0,
                    decimal,
                    multispace0,
                    tag(")"),
                ),
            ),
        )),
    ))
    .parse(input)
}

fn col_def(input: &str) -> ParseResult<Column> {
    map(
        separated_pair(ident, multispace1, datatype),
        |(name, datatype)| Column { name, datatype },
    )
    .parse(input)
}

fn show_tables(input: &str) -> ParseResult<SqlStmt> {
    value(
        SqlStmt::ShowTables,
        (kw_preceded("SHOW"), tag_no_case("TABLES")),
    )
    .parse(input)
}

fn describe(input: &str) -> ParseResult<SqlStmt> {
    map(
        preceded(alt((kw_preceded("DESCRIBE"), kw_preceded("DESC"))), ident),
        |table| SqlStmt::Describe { table },
    )
    .parse(input)
}

fn create_table(input: &str) -> ParseResult<SqlStmt> {
    map(
        preceded(
            (kw_preceded("CREATE"), kw_preceded("TABLE")),
            (
                ident,
                delimited(
                    (multispace0, tag("("), multispace0),
                    comma_list1(col_def),
                    (multispace0, tag(")")),
                ),
            ),
        ),
        |(table, columns)| SqlStmt::CreateTable { table, columns },
    )
    .parse(input)
}

fn columns(input: &str) -> ParseResult<Vec<SqlCol>> {
    comma_list1(col).parse(input)
}

fn integer(input: &str) -> ParseResult<i64> {
    nom::character::complete::i64(input)
}

fn decimal(input: &str) -> ParseResult<&str> {
    recognize(many1(terminated(one_of("0123456789"), many0(tag("_"))))).parse(input)
}

fn real(input: &str) -> ParseResult<f64> {
    map_res(
        alt((
            // Case one: .42
            recognize((
                tag("."),
                decimal,
                opt((one_of("eE"), opt(one_of("+-")), decimal)),
            )), // Case two: 42e42 and 42.42e42
            recognize((
                decimal,
                opt(preceded(tag("."), decimal)),
                one_of("eE"),
                opt(one_of("+-")),
                decimal,
            )), // Case three: 42. and 42.42
            recognize((decimal, tag("."), opt(decimal))),
        )),
        |s| s.parse(),
    )
    .parse(input)
}

fn text(input: &str) -> ParseResult<String> {
    delimited(
        tag("\""),
        fold_many0(
            alt((
                preceded(
                    tag("\\"),
                    alt((
                        map(one_of("nrt\\\""), |escape| match escape {
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            '\\' => '\\',
                            '\"' => '\"',
                            _ => unreachable!(),
                        }),
                        map_opt(delimited(tag("{"), hex_u32, tag("}")), |unicode| {
                            char::from_u32(unicode)
                        }),
                    )),
                ),
                none_of("\\\""),
            )),
            String::new,
            |mut s, c| {
                s.push(c);
                s
            },
        ),
        tag("\""),
    )
    .parse(input)
}

fn const_(input: &str) -> ParseResult<Value> {
    alt((
        value(Value::Null, tag_no_case("NULL")),
        map(integer, Value::Integer),
        map(real, Value::Real),
        map(text, Value::Text),
    ))
    .parse(input)
}

fn values(input: &str) -> ParseResult<Vec<Vec<Value>>> {
    comma_list1(paren(comma_list1(const_))).parse(input)
}

fn insert_into(input: &str) -> ParseResult<SqlStmt> {
    map(
        preceded(
            (kw_preceded("INSERT"), kw_preceded("INTO")),
            (
                ident,
                preceded(
                    multispace0,
                    (paren(columns), preceded(kw("VALUES"), values)),
                ),
            ),
        ),
        |(table, (columns, values))| SqlStmt::InsertInto {
            table,
            columns,
            values,
        },
    )
    .parse(input)
}

fn from(input: &str) -> ParseResult<String> {
    map(preceded(kw("FROM"), ident), |table| table).parse(input)
}

fn join_on(input: &str) -> ParseResult<(String, SqlOn)> {
    map(
        preceded(
            kw("JOIN"),
            (
                ident,
                preceded(
                    kw("ON"),
                    separated_pair(col, (multispace0, tag("="), multispace0), col),
                ),
            ),
        ),
        |(table, (lhs, rhs))| (table, SqlOn { lhs, rhs }),
    )
    .parse(input)
}

fn col_or_const(input: &str) -> ParseResult<SqlColOrExpr> {
    alt((
        map(col, SqlColOrExpr::Column),
        map(const_, SqlColOrExpr::Const),
    ))
    .parse(input)
}

fn where_rel(input: &str) -> ParseResult<SqlRel> {
    alt((
        map(
            (
                col_or_const,
                delimited(multispace0, alt((tag("="), tag("<="))), multispace0),
                col_or_const,
            ),
            |(lhs, op, rhs)| match op {
                "=" => SqlRel::Eq { lhs, rhs },
                "<=" => SqlRel::Le { lhs, rhs },
                _ => unreachable!(),
            },
        ),
        map(separated_pair(col, kw("LIKE"), text), |(lhs, rhs)| {
            SqlRel::Like { lhs, rhs }
        }),
    ))
    .parse(input)
}

fn where_clause(input: &str) -> ParseResult<SqlWhere> {
    precedence(
        unary_op(1, kw("NOT")),
        fail(),
        alt((
            binary_op(2, Assoc::Left, kw("AND")),
            binary_op(2, Assoc::Left, kw("OR")),
        )),
        alt((
            map(where_rel, SqlWhere::Rel),
            delimited(tag("("), where_clause, tag(")")),
        )),
        |op: Operation<&str, &str, &str, SqlWhere>| -> Result<SqlWhere> {
            use nom_language::precedence::Operation::*;
            match op {
                Prefix(_, clause) => Ok(SqlWhere::Not(Box::new(clause))),
                Binary(lhs, op, rhs) => match op.to_uppercase().as_str() {
                    "AND" => Ok(SqlWhere::And(Box::new(lhs), Box::new(rhs))),
                    "OR" => Ok(SqlWhere::Or(Box::new(lhs), Box::new(rhs))),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        },
    )
    .parse(input)
}

fn where_(input: &str) -> ParseResult<SqlWhere> {
    preceded(kw("WHERE"), where_clause).parse(input)
}

fn limit(input: &str) -> ParseResult<u64> {
    preceded(kw("LIMIT"), nom::character::complete::u64).parse(input)
}

fn select_target(input: &str) -> ParseResult<SqlSelectTarget> {
    alt((
        map(col, SqlSelectTarget::Column),
        map(const_, SqlSelectTarget::Const),
        value(SqlSelectTarget::Wildcard, tag("*")),
        map(recognize((alt((tag("@@"), tag("@"))), ident)), |variable| {
            SqlSelectTarget::Variable(variable.to_owned())
        }),
    ))
    .parse(input)
}

fn select(input: &str) -> ParseResult<SqlStmt> {
    map(
        preceded(
            kw_preceded("SELECT"),
            (
                comma_list1(select_target),
                opt(from),
                many0(join_on),
                opt(where_),
                opt(limit),
            ),
        ),
        |(columns, table, join_on, where_, limit)| SqlStmt::Select {
            columns,
            table,
            join_on,
            where_,
            limit,
        },
    )
    .parse(input)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_create_table() {
        assert_eq!(
            format!(
                "{:?}",
                Aidb::parse("CREATE TABLE students (id INTEGER, name TEXT);").unwrap()
            ),
            r#"CreateTable { table: "students", columns: [SqlColDef { name: "id", datatype: Integer }, SqlColDef { name: "name", datatype: Text }] }"#
        );
    }

    #[test]
    fn test_insert_into() {
        assert_eq!(
            format!(
                "{:?}",
                Aidb::parse(r#"INSERT INTO students(id, name) VALUES (42, "Alice"), (43, "Bob");"#)
                    .unwrap()
            ),
            r#"InsertInto { table: "students", columns: [Short("id"), Short("name")], values: [[Integer(42), Text("Alice")], [Integer(43), Text("Bob")]] }"#
        );
    }

    #[test]
    fn test_select() {
        assert_eq!(
            format!(
                "{:?}",
                Aidb::parse(r#"SELECT students.name, classes.class FROM students JOIN classes ON students.id = classes.student_id WHERE students.name LIKE "张%";"#)
                    .unwrap()
            ),
            r#"Select { columns: [Column(Full { table: "students", column: "name" }), Column(Full { table: "classes", column: "class" })], table: Some("students"), join_on: [("classes", SqlOn { lhs: Full { table: "students", column: "id" }, rhs: Full { table: "classes", column: "student_id" } })], where_: Some(Rel(Like { lhs: Full { table: "students", column: "name" }, rhs: "张%" })) }"#
        );
    }
}
