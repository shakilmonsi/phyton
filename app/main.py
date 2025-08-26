
import argparse
from . import db
from . import crud

def do_init_schema():
    print("Initializing schema...")
    db.init_schema()
    print("Schema created.")

def do_demo():
    print("Running demo...")
    # Insert via CRUD
    bid = crud.add_book("Oracle with Python", "Shahin Shaukat", "Y")
    print(f"Inserted book id: {bid}")
    # List
    print("Books:", crud.list_books())
    # Update
    print("Update availability to 'N'...")
    crud.update_availability(bid, "N")
    print("Books:", crud.list_books())
    # Call procedure
    print("Call procedure add_book(...)")
    crud.call_add_book_proc("Effective Java", "Joshua Bloch", "Y")
    # Call function
    total = crud.call_get_book_count_func()
    print("Total books via function:", total)
    # Delete
    print("Delete first book id:", bid)
    crud.delete_book(bid)
    print("Books:", crud.list_books())
    print("Demo complete.")

def do_list():
    for row in crud.list_books():
        print(row)

def main():
    parser = argparse.ArgumentParser(description="Oracle + Python starter")
    parser.add_argument("--init-schema", action="store_true", help="Create tables/procedure/function/trigger")
    parser.add_argument("--demo", action="store_true", help="Run CRUD + PL/SQL demo")
    parser.add_argument("--list", action="store_true", help="List books")
    args = parser.parse_args()

    if args.init_schema:
        do_init_schema()
    if args.demo:
        do_demo()
    if args.list:
        do_list()
    if not any([args.init_schema, args.demo, args.list]):
        parser.print_help()

if __name__ == "__main__":
    main()
