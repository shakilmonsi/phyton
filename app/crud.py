
from typing import List, Tuple, Optional

try:
    import oracledb as ora
except ImportError:
    import cx_Oracle as ora  # type: ignore

from .db import get_connection

def add_book(title: str, author: str, available: str = "Y") -> int:
    sql = "INSERT INTO books (title, author, available) VALUES (:1, :2, :3) RETURNING id INTO :4"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            out_id = cur.var(ora.NUMBER)
            cur.execute(sql, (title, author, available, out_id))
        conn.commit()
        return int(out_id.getvalue())
    finally:
        conn.close()

def list_books(limit: int = 100) -> List[Tuple]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, title, author, available FROM books ORDER BY id FETCH FIRST :n ROWS ONLY", [limit])
            return cur.fetchall()
    finally:
        conn.close()

def update_availability(book_id: int, available: str) -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE books SET available = :1 WHERE id = :2", (available, book_id))
            rowcount = cur.rowcount
        conn.commit()
        return rowcount or 0
    finally:
        conn.close()

def delete_book(book_id: int) -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM books WHERE id = :1", [book_id])
            rowcount = cur.rowcount
        conn.commit()
        return rowcount or 0
    finally:
        conn.close()

def call_add_book_proc(title: str, author: str, available: str = "Y"):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.callproc("add_book", [title, author, available])
        conn.commit()
    finally:
        conn.close()

def call_get_book_count_func() -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            total = cur.callfunc("get_book_count", ora.NUMBER, [])
            return int(total)
    finally:
        conn.close()
