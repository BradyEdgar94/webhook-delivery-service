//! Cursor (keyset) pagination.
//!
//! Every list route returns newest-first pages and a `next_cursor`. The cursor
//! is just the id of the last row on the page (base64-encoded so clients treat
//! it as opaque), and the next page is `WHERE id < cursor ORDER BY id DESC`.
//!
//! **Why not `OFFSET`?** `OFFSET 100000` makes Postgres walk and discard
//! 100,000 rows, so deep pages get slower and slower. And if rows are inserted
//! while a client pages through, offsets shift: rows get skipped or repeated.
//! A keyset cursor uses the index to jump straight to the right place, and
//! because ids only grow, newly inserted rows never disturb a page sequence
//! already in progress: every row that existed when paging started is
//! returned exactly once.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use super::error::ApiError;

pub const DEFAULT_LIMIT: i64 = 50;
pub const MAX_LIMIT: i64 = 200;

#[derive(Debug, Deserialize)]
pub struct PageParams {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

impl PageParams {
    pub fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(DEFAULT_LIMIT),
            Some(n) if (1..=MAX_LIMIT).contains(&n) => Ok(n),
            Some(_) => Err(ApiError::bad_request(format!(
                "limit must be between 1 and {MAX_LIMIT}"
            ))),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Page<T> {
    pub data: Vec<T>,
    pub next_cursor: Option<String>,
}

pub fn encode_cursor(id: &str) -> String {
    URL_SAFE_NO_PAD.encode(id.as_bytes())
}

/// Decode a cursor into the id type of the list (`Uuid` or `i64`).
pub fn decode_cursor<T: std::str::FromStr>(cursor: Option<&str>) -> Result<Option<T>, ApiError> {
    let Some(c) = cursor else { return Ok(None) };
    let bytes = URL_SAFE_NO_PAD
        .decode(c)
        .map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let s = String::from_utf8(bytes).map_err(|_| ApiError::bad_request("invalid cursor"))?;
    s.parse::<T>()
        .map(Some)
        .map_err(|_| ApiError::bad_request("invalid cursor"))
}

/// Build a page from `limit + 1` fetched rows: the extra row only tells us
/// whether another page exists, and is not returned.
pub fn page<R, T>(
    mut rows: Vec<R>,
    limit: i64,
    id_of: impl Fn(&R) -> String,
    view: impl Fn(R) -> T,
) -> Page<T> {
    let has_more = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    let next_cursor = if has_more {
        rows.last().map(|r| encode_cursor(&id_of(r)))
    } else {
        None
    };
    Page {
        data: rows.into_iter().map(view).collect(),
        next_cursor,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn cursor_round_trip_i64() {
        let c = encode_cursor("12345");
        assert_eq!(decode_cursor::<i64>(Some(&c)).unwrap(), Some(12345));
    }

    #[test]
    fn cursor_round_trip_uuid() {
        let id = Uuid::now_v7();
        let c = encode_cursor(&id.to_string());
        assert_eq!(decode_cursor::<Uuid>(Some(&c)).unwrap(), Some(id));
    }

    #[test]
    fn cursor_is_opaque_and_url_safe() {
        let c = encode_cursor(&Uuid::now_v7().to_string());
        assert!(
            c.chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
        );
    }

    #[test]
    fn bad_cursors_rejected() {
        assert!(decode_cursor::<i64>(Some("!!!")).is_err());
        assert!(decode_cursor::<i64>(Some(&encode_cursor("abc"))).is_err());
        assert_eq!(decode_cursor::<i64>(None).unwrap(), None);
    }

    #[test]
    fn page_uses_extra_row_to_decide_next_cursor() {
        let p = page(vec![5i64, 4, 3], 2, |r| r.to_string(), |r| r);
        assert_eq!(p.data, vec![5, 4]);
        assert_eq!(
            decode_cursor::<i64>(p.next_cursor.as_deref()).unwrap(),
            Some(4)
        );
        let last = page(vec![2i64, 1], 2, |r| r.to_string(), |r| r);
        assert!(last.next_cursor.is_none());
    }

    #[test]
    fn limit_bounds() {
        assert_eq!(
            PageParams {
                cursor: None,
                limit: None
            }
            .limit()
            .unwrap(),
            DEFAULT_LIMIT
        );
        assert!(
            PageParams {
                cursor: None,
                limit: Some(0)
            }
            .limit()
            .is_err()
        );
        assert!(
            PageParams {
                cursor: None,
                limit: Some(201)
            }
            .limit()
            .is_err()
        );
    }
}
