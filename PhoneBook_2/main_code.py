import psycopg2
from psycopg2 import sql
import sys

class PhoneBookAdvanced:
    def __init__(self):
        self.conn_params = {
            'host': 'localhost',
            'database': 'postgres',
            'user': 'postgres',
            'password': 'Muhammed4ever'  # Replace with your actual password
        }
        self.conn = None
        self.connect()
        self.create_tables()
        self.create_functions_procedures()

    def connect(self):
        """Connect to the PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            print("Connected to PostgreSQL database!")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error connecting to PostgreSQL: {error}")
            sys.exit(1)

    def create_tables(self):
        """Create phonebook table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS phonebook (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50),
            phone VARCHAR(20) NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_table_sql)
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error creating table: {error}")
            self.conn.rollback()

    def create_functions_procedures(self):
        """Create all required functions and stored procedures"""
        procedures = [
            # 1. Function to search by pattern
            """
            CREATE OR REPLACE FUNCTION search_contacts(pattern TEXT)
            RETURNS TABLE (
                id INTEGER,
                first_name VARCHAR,
                last_name VARCHAR,
                phone VARCHAR,
                created_at TIMESTAMP
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT * FROM phonebook
                WHERE first_name ILIKE '%' || pattern || '%'
                   OR last_name ILIKE '%' || pattern || '%'
                   OR phone ILIKE '%' || pattern || '%';
            END;
            $$ LANGUAGE plpgsql;
            """,
            
            # 2. Procedure to insert or update user
            """
            CREATE OR REPLACE PROCEDURE upsert_user(
                f_name VARCHAR, 
                l_name VARCHAR, 
                user_phone VARCHAR
            )
            AS $$
            BEGIN
                INSERT INTO phonebook (first_name, last_name, phone)
                VALUES (f_name, l_name, user_phone)
                ON CONFLICT (phone) 
                DO UPDATE SET 
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name;
            END;
            $$ LANGUAGE plpgsql;
            """,
            
            # 3. Procedure to insert many users with validation
            """
            CREATE OR REPLACE PROCEDURE insert_many_users(
                IN names_phones TEXT[],
                OUT invalid_data TEXT[]
            )
            AS $$
            DECLARE
                item TEXT;
                parts TEXT[];
                f_name TEXT;
                l_name TEXT;
                phone TEXT;
                phone_valid BOOLEAN;
            BEGIN
                invalid_data := '{}';
                
                FOREACH item IN ARRAY names_phones LOOP
                    parts := string_to_array(item, ',');
                    
                    IF array_length(parts, 1) = 3 THEN
                        f_name := trim(parts[1]);
                        l_name := trim(parts[2]);
                        phone := trim(parts[3]);
                        
                        -- Simple phone validation (10 digits)
                        phone_valid := phone ~ '^[0-9]{10}$';
                        
                        IF phone_valid THEN
                            CALL upsert_user(f_name, l_name, phone);
                        ELSE
                            invalid_data := array_append(invalid_data, item);
                        END IF;
                    ELSE
                        invalid_data := array_append(invalid_data, item);
                    END IF;
                END LOOP;
            END;
            $$ LANGUAGE plpgsql;
            """,
            
            # 4. Function for pagination
            """
            CREATE OR REPLACE FUNCTION get_contacts_paginated(
                lim INTEGER,
                offs INTEGER
            )
            RETURNS TABLE (
                id INTEGER,
                first_name VARCHAR,
                last_name VARCHAR,
                phone VARCHAR,
                created_at TIMESTAMP
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT * FROM phonebook
                ORDER BY first_name, last_name
                LIMIT lim OFFSET offs;
            END;
            $$ LANGUAGE plpgsql;
            """,
            
            # 5. Procedure to delete by name or phone
            """
            CREATE OR REPLACE PROCEDURE delete_contact(
                search_term TEXT
            )
            AS $$
            BEGIN
                DELETE FROM phonebook
                WHERE first_name ILIKE search_term
                   OR last_name ILIKE search_term
                   OR phone ILIKE search_term;
            END;
            $$ LANGUAGE plpgsql;
            """
        ]
        
        try:
            with self.conn.cursor() as cur:
                for procedure in procedures:
                    cur.execute(procedure)
                self.conn.commit()
                print("All functions and procedures created successfully!")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error creating functions/procedures: {error}")
            self.conn.rollback()

    def search_by_pattern(self, pattern):
        """Search contacts using the pattern search function"""
        try:
            with self.conn.cursor() as cur:
                cur.callproc('search_contacts', (pattern,))
                results = cur.fetchall()
                
                if not results:
                    print("No contacts found matching the pattern.")
                    return
                
                print("\nSearch Results:")
                print("{:<5} {:<15} {:<15} {:<15} {:<20}".format(
                    "ID", "First Name", "Last Name", "Phone", "Created At"))
                print("-" * 70)
                for contact in results:
                    print("{:<5} {:<15} {:<15} {:<15} {:<20}".format(
                        contact[0],
                        contact[1] or '',
                        contact[2] or '',
                        contact[3] or '',
                        str(contact[4]) if contact[4] else ''
                    ))
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error searching contacts: {error}")

    def upsert_contact(self, first_name, last_name, phone):
        """Insert or update contact using the stored procedure"""
        try:
            with self.conn.cursor() as cur:
                cur.callproc('upsert_user', (first_name, last_name, phone))
                self.conn.commit()
                print("Contact upserted successfully!")
        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            print(f"Error upserting contact: {error}")

    def insert_many_contacts(self, contacts_list):
        """
        Insert multiple contacts with validation
        Each contact should be in format: "first_name,last_name,phone"
        """
        try:
            with self.conn.cursor() as cur:
                # Call the procedure with OUT parameter
                cur.callproc('insert_many_users', (contacts_list, None))
                invalid_data = cur.fetchone()[0]
                
                self.conn.commit()
                
                if invalid_data:
                    print("\nInvalid data that wasn't inserted:")
                    for item in invalid_data:
                        print(f"- {item}")
                else:
                    print("All contacts inserted successfully!")
        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            print(f"Error inserting multiple contacts: {error}")

    def get_paginated_contacts(self, limit, offset):
        """Get contacts with pagination"""
        try:
            with self.conn.cursor() as cur:
                cur.callproc('get_contacts_paginated', (limit, offset))
                results = cur.fetchall()
                
                if not results:
                    print("No contacts found in this range.")
                    return
                
                print(f"\nContacts {offset + 1} to {offset + len(results)}:")
                print("{:<5} {:<15} {:<15} {:<15}".format(
                    "ID", "First Name", "Last Name", "Phone"))
                print("-" * 50)
                for contact in results:
                    print("{:<5} {:<15} {:<15} {:<15}".format(
                        contact[0],
                        contact[1] or '',
                        contact[2] or '',
                        contact[3] or ''
                    ))
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error getting paginated contacts: {error}")

    def delete_by_search_term(self, search_term):
        """Delete contacts by name or phone"""
        try:
            with self.conn.cursor() as cur:
                # First show what will be deleted
                cur.execute("""
                    SELECT * FROM phonebook
                    WHERE first_name ILIKE %s
                       OR last_name ILIKE %s
                       OR phone ILIKE %s
                """, (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%'))
                to_delete = cur.fetchall()
                
                if not to_delete:
                    print("No contacts found matching the search term.")
                    return
                
                print("\nThe following contacts will be deleted:")
                for contact in to_delete:
                    print(f"ID: {contact[0]}, Name: {contact[1]} {contact[2]}, Phone: {contact[3]}")
                
                confirm = input("\nAre you sure you want to delete these contacts? (y/n): ").strip().lower()
                if confirm != 'y':
                    print("Deletion cancelled.")
                    return
                
                # Call the delete procedure
                cur.callproc('delete_contact', (f'%{search_term}%',))
                self.conn.commit()
                print(f"Deleted {len(to_delete)} contact(s).")
        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            print(f"Error deleting contacts: {error}")

    def close(self):
        """Close the database connection"""
        if self.conn is not None:
            self.conn.close()
            print("Database connection closed.")

def main():
    phonebook = PhoneBookAdvanced()
    
    while True:
        print("\nPhoneBook Advanced Management System")
        print("1. Search contacts by pattern")
        print("2. Insert/update contact")
        print("3. Insert multiple contacts")
        print("4. View contacts with pagination")
        print("5. Delete contacts by name or phone")
        print("6. Exit")
        
        choice = input("Enter your choice (1-6): ").strip()
        
        if choice == '1':
            pattern = input("Enter search pattern: ").strip()
            phonebook.search_by_pattern(pattern)
        elif choice == '2':
            first_name = input("First name: ").strip()
            last_name = input("Last name (optional): ").strip() or None
            phone = input("Phone number: ").strip()
            phonebook.upsert_contact(first_name, last_name, phone)
        elif choice == '3':
            print("Enter contacts in format: first_name,last_name,phone (one per line)")
            print("Enter 'done' when finished")
            contacts = []
            while True:
                contact = input("> ").strip()
                if contact.lower() == 'done':
                    break
                contacts.append(contact)
            phonebook.insert_many_contacts(contacts)
        elif choice == '4':
            limit = int(input("Number of contacts per page: ").strip())
            offset = int(input("Offset (start from): ").strip())
            phonebook.get_paginated_contacts(limit, offset)
        elif choice == '5':
            search_term = input("Enter name or phone to delete: ").strip()
            phonebook.delete_by_search_term(search_term)
        elif choice == '6':
            phonebook.close()
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()