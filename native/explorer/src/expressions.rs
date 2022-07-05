// The idea of this file is to have functions that
// transform the expressions from the Elixir side
// to the Rust side. It's a conversion of Elixir tuples
// in the following format: `{:operation, args}` to
// the Polars expressions.

use polars::prelude::col;
use polars::prelude::Expr;
use polars::prelude::Literal;
use rustler::types::atom::Atom;
use rustler::Term;

use crate::ExplorerError;

rustler::atoms! {
  equal,
  column
}

pub fn term_to_expressions(term: Term) -> Result<Expr, ExplorerError> {
    if term.is_tuple() {
        let (key, args): (Atom, Term) = match term.decode() {
            Ok(pair) => pair,
            Err(_) => return Err(ExplorerError::Other("cannot read operation 1".to_string())),
        };

        if key == column() {
            // let (name, _) = match args.decode() {
            //     Ok(pair) => pair,
            //     Err(_) => {
            //         return Err(ExplorerError::Other(
            //             "cannot read operation 2.0".to_string(),
            //         ))
            //     }
            // };

            let decoded_name: &str = match args.decode() {
                Ok(value) => value,
                Err(_) => return Err(ExplorerError::Other("cannot read operation 2".to_string())),
            };
            return Ok(col(decoded_name));
        }

        if key == equal() {
            let (left, tail) = match args.list_get_cell() {
                Ok(pair) => pair,
                Err(_) => return Err(ExplorerError::Other("cannot read operation 4".to_string())),
            };
            let (right, _) = match tail.list_get_cell() {
                Ok(pair) => pair,
                Err(_) => return Err(ExplorerError::Other("cannot read operation 5".to_string())),
            };

            let left_exp = term_to_expressions(left)?;
            let right_exp = term_to_expressions(right)?;
            return Ok(left_exp.eq(right_exp));
        }
    }

    if term.is_number() {
        // TODO: make the case with floats pass
        let int: i64 = match term.decode() {
            Ok(value) => value,
            Err(_) => return Err(ExplorerError::Other("cannot read operation 3".to_string())),
        };

        return Ok(int.lit());
    }

    return Err(ExplorerError::Other(
        "cannot convert to expressions".to_string(),
    ));
}

// fn get_head_tail(term: Term) -> Result<(Term, Term), ExplorerError:> {
//     match term.list_get_cell() {
//         Ok(pair) => Ok(pair),
//         Err(_) => Err(ExplorerError::Other("cannot read operation 4".to_string())),
//     }
// }
