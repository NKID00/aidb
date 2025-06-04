use eyre::{Result, eyre};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1, none_of, one_of},
    combinator::{eof, map, map_opt, map_res, opt, recognize, value},
    error::ParseError,
    multi::{fold_many0, many0, many0_count, many1, separated_list0, separated_list1},
    number::complete::hex_u32,
    sequence::{delimited, preceded, separated_pair, terminated},
};

use crate::{DataType, Value};

#[derive(Debug, Clone)]
pub enum SqlStmt {
    /// CREATE TABLE table (column datatype, ...)
    CreateTable {
        table: String,
        columns: Vec<SqlColDef>,
    },
    /// INSERT INTO table [(column, ...)] VALUES value, ...
    InsertInto {
        table: String,
        columns: Vec<SqlCol>,
        values: Vec<Vec<Value>>,
    },
    /// SELECT column, ... [FROM table] [JOIN table ON condition ...] [WHERE condition]
    Select {
        columns: Vec<SqlColOrExpr>,
        table: Option<String>,
        join_on: Vec<(String, SqlOn)>,
        where_: Option<SqlWhere>,
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
pub struct SqlColDef {
    pub name: String,
    pub datatype: DataType,
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
pub enum SqlColOrExpr {
    Column(SqlCol),
    Const(Value),
}

#[derive(Debug, Clone)]
pub enum SqlWhere {
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
    And(Box<SqlWhere>, Box<SqlWhere>),
    Or(Box<SqlWhere>, Box<SqlWhere>),
    Not(Box<SqlWhere>),
}

pub fn complete(input: impl AsRef<str>) -> String {
    "hint1".to_owned()
}

pub fn parse(input: impl AsRef<str>) -> Result<SqlStmt> {
    match stmt(input.as_ref()) {
        Ok((remain, stmt)) => {
            assert!(remain.is_empty());
            Ok(stmt)
        }
        Err(e) => Err(eyre!("SQL parse error: {:?}", e)),
    }
}

fn kw_preceded<'a, 'b, E: ParseError<&'a str>>(
    kw: &'b str,
) -> impl Parser<&'a str, Output = (), Error = E> {
    map((tag_no_case(kw), multispace1), |_| ())
}

fn kw<'a, 'b, E: ParseError<&'a str>>(kw: &'b str) -> impl Parser<&'a str, Output = (), Error = E> {
    map((multispace1, tag_no_case(kw), multispace1), |_| ())
}

fn comma_list0<'a, T, E: ParseError<&'a str>>(
    parser: impl Parser<&'a str, Output = T, Error = E>,
) -> impl Parser<&'a str, Output = Vec<T>, Error = E> {
    separated_list0((multispace0, tag(","), multispace0), parser)
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

fn ident(input: &str) -> IResult<&str, &str> {
    recognize((
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    ))
    .parse(input)
}

fn col(input: &str) -> IResult<&str, SqlCol> {
    alt((
        map(separated_pair(ident, tag("."), ident), |(table, column)| {
            SqlCol::Full {
                table: table.to_owned(),
                column: column.to_owned(),
            }
        }),
        map(ident, |s| SqlCol::Short(s.to_owned())),
    ))
    .parse(input)
}

fn stmt(input: &str) -> IResult<&str, SqlStmt> {
    delimited(
        multispace0,
        alt((create_table, insert_into, select)),
        (multispace0, opt(tag(";")), multispace0, eof),
    )
    .parse(input)
}

fn datatype(input: &str) -> IResult<&str, DataType> {
    use DataType::*;
    alt((
        value(Integer, tag_no_case("INTEGER")),
        value(Real, tag_no_case("REAL")),
        value(Text, tag_no_case("TEXT")),
    ))
    .parse(input)
}

fn col_def(input: &str) -> IResult<&str, SqlColDef> {
    map(
        separated_pair(ident, multispace1, datatype),
        |(name, datatype)| SqlColDef {
            name: name.to_owned(),
            datatype,
        },
    )
    .parse(input)
}

fn create_table(input: &str) -> IResult<&str, SqlStmt> {
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
        |(table, columns)| SqlStmt::CreateTable {
            table: table.to_owned(),
            columns,
        },
    )
    .parse(input)
}

fn columns(input: &str) -> IResult<&str, Vec<SqlCol>> {
    comma_list1(col).parse(input)
}

fn integer(input: &str) -> IResult<&str, i64> {
    nom::character::complete::i64(input)
}

fn decimal(input: &str) -> IResult<&str, &str> {
    recognize(many1(terminated(one_of("0123456789"), many0(tag("_"))))).parse(input)
}

fn real(input: &str) -> IResult<&str, f64> {
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

fn text(input: &str) -> IResult<&str, String> {
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

fn const_(input: &str) -> IResult<&str, Value> {
    alt((
        value(Value::Null, tag_no_case("NULL")),
        map(integer, |v| Value::Integer(v)),
        map(real, |v| Value::Real(v)),
        map(text, |v| Value::Text(v)),
    ))
    .parse(input)
}

fn values(input: &str) -> IResult<&str, Vec<Vec<Value>>> {
    comma_list1(paren(comma_list1(const_))).parse(input)
}

fn insert_into(input: &str) -> IResult<&str, SqlStmt> {
    map(
        preceded(
            (kw_preceded("INSERT"), kw_preceded("INTO")),
            (ident, paren(columns), preceded(kw("VALUES"), values)),
        ),
        |(table, columns, values)| SqlStmt::InsertInto {
            table: table.to_owned(),
            columns,
            values,
        },
    )
    .parse(input)
}

fn from(input: &str) -> IResult<&str, String> {
    map(preceded(kw("FROM"), ident), |table| table.to_owned()).parse(input)
}

fn join_on(input: &str) -> IResult<&str, (String, SqlOn)> {
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
        |(table, (lhs, rhs))| (table.to_owned(), SqlOn { lhs, rhs }),
    )
    .parse(input)
}

fn col_or_const(input: &str) -> IResult<&str, SqlColOrExpr> {
    alt((
        map(col, |column| SqlColOrExpr::Column(column)),
        map(const_, |v| SqlColOrExpr::Const(v)),
    ))
    .parse(input)
}

fn where_clause(input: &str) -> IResult<&str, SqlWhere> {
    alt((
        map(
            (
                col_or_const,
                delimited(multispace0, alt((tag("="), tag("<="))), multispace0),
                col_or_const,
            ),
            |(lhs, op, rhs)| match op {
                "=" => SqlWhere::Eq { lhs, rhs },
                "<=" => SqlWhere::Le { lhs, rhs },
                _ => unreachable!(),
            },
        ),
        map(separated_pair(col, kw("LIKE"), text), |(lhs, rhs)| {
            SqlWhere::Like { lhs, rhs }
        }),
        map(
            (
                where_clause,
                delimited(
                    multispace0,
                    alt((tag_no_case("AND"), tag_no_case("OR"))),
                    multispace0,
                ),
                where_clause,
            ),
            |(lhs, op, rhs)| match op.to_uppercase().as_str() {
                "AND" => SqlWhere::And(Box::new(lhs), Box::new(rhs)),
                "OR" => SqlWhere::Or(Box::new(lhs), Box::new(rhs)),
                _ => unreachable!(),
            },
        ),
    ))
    .parse(input)
}

fn where_(input: &str) -> IResult<&str, SqlWhere> {
    preceded(kw("WHERE"), where_clause).parse(input)
}

fn select(input: &str) -> IResult<&str, SqlStmt> {
    map(
        preceded(
            kw_preceded("SELECT"),
            (comma_list1(col_or_const), opt(from), many0(join_on), opt(where_)),
        ),
        |(columns, table, join_on, where_)| SqlStmt::Select {
            columns,
            table,
            join_on,
            where_,
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
                parse("CREATE TABLE students (id INTEGER, name TEXT);").unwrap()
            ),
            r#"CreateTable { table: "students", columns: [SqlColDef { name: "id", datatype: Integer }, SqlColDef { name: "name", datatype: Text }] }"#
        );
    }

    #[test]
    fn test_insert_into() {
        assert_eq!(
            format!(
                "{:?}",
                parse(r#"INSERT INTO students(id, name) VALUES (42, "Alice"), (43, "Bob");"#)
                    .unwrap()
            ),
            r#"InsertInto { table: "students", columns: [Short("id"), Short("name")], values: [[Int(42), Text("Alice")], [Int(43), Text("Bob")]] }"#
        );
    }

    #[test]
    fn test_select() {
        assert_eq!(
            format!(
                "{:?}",
                parse(r#"SELECT students.name, classes.class FROM students JOIN classes ON students.id = classes.student_id WHERE students.name LIKE "张%";"#)
                    .unwrap()
            ),
            r#"Select { columns: [Full { table: "students", column: "name" }, Full { table: "classes", column: "class" }], table: Some("students"), join_on: [("classes", SqlOn { lhs: Full { table: "students", column: "id" }, rhs: Full { table: "classes", column: "student_id" } })], where_: Some(Like { lhs: Full { table: "students", column: "name" }, rhs: "张%" }) }"#
        );
    }
}
