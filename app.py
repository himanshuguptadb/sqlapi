from flask import Flask, request, jsonify, render_template
from flask_restful import Api, Resource
from enum import Enum
from urllib.parse import urlparse
import os
import requests
import json
import random
import subprocess
import threading
import time

# Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import *

# Initialize Databricks SDK. 
# By default, this authenticates using the DATABRICKS_HOST and DATABRICKS_TOKEN environment variables
# initialized by setup.sh.
w = WorkspaceClient()

warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")

app = Flask(__name__)
api = Api(app)

# Sample data
books = [
    {"id": 1, "title": "Python Basics", "author": "John Doe"},
    {"id": 2, "title": "Flask Web Development", "author": "Jane Smith"}
]

w = WorkspaceClient()

class SqlStatement(Enum):
    LIST_STORES = 1
    LIST_SALES = 2

sql_statements = {
     SqlStatement.LIST_STORES: """
            select
                id,
                name, 
                manager,
                employee_count
                city,
                state
            from 
                acme_demo_stores
        """,
    SqlStatement.LIST_SALES: """
            select 
                date,
                id,
                store_id,
                item_id,
                quantity,
                price
            from 
                acme_demo_sales
            where
                store_id = :store_id
            order by
                date desc
        """
}

class stores(Resource):
    def get(self):
        statement_response = w.statement_execution.execute_statement(
        statement = sql_statements[SqlStatement.LIST_STORES],
        wait_timeout = "50s",
        on_wait_timeout = TimeoutAction.CANCEL,
        warehouse_id = warehouse_id)

        stores = None
        if statement_response.status.state == StatementState.SUCCEEDED:
            stores = statement_response.result.data_array
        else:
            print(statement_response.status)

        response = {
        'state': str(statement_response.status.state.name),
        'stores': stores 
        }
    
        return jsonify(response)

class store_sales(Resource):
    def get(self,store_id):
        # Use parameters to prevent SQL injection via the store ID string.
        parameters = [
        StatementParameterListItem(name='store_id', value=store_id, type="INT")
        ]

        statement_response = w.statement_execution.execute_statement(
        statement = sql_statements[SqlStatement.LIST_SALES],
        wait_timeout = "50s",
        warehouse_id = warehouse_id,
        parameters = parameters,
        on_wait_timeout = TimeoutAction.CANCEL
        )

        if statement_response.status.state == StatementState.SUCCEEDED:
            store_sales = statement_response.result.data_array
        else:
            print(statement_response.status)

        response = {
        'state': str(statement_response.status.state.name),
        'store_sales': store_sales 
        }
    
        return jsonify(response)

class Books(Resource):
    def get(self):
        return {"books": books}, 200
    
    def post(self):
        new_book = request.get_json()
        new_book["id"] = len(books) + 1
        books.append(new_book)
        return new_book, 201

class Book(Resource):
    def get(self, book_id):
        book = next((book for book in books if book["id"] == book_id), None)
        if book:
            return book, 200
        return {"message": "Book not found"}, 404
    
    def put(self, book_id):
        book = next((book for book in books if book["id"] == book_id), None)
        if book:
            updated_book = request.get_json()
            book.update(updated_book)
            return book, 200
        return {"message": "Book not found"}, 404
    
    def delete(self, book_id):
        global books
        books = [book for book in books if book["id"] != book_id]
        return {"message": "Book deleted"}, 200

api.add_resource(Books, '/books')
api.add_resource(Book, '/books/<int:book_id>')
api.add_resource(stores, '/stores')
api.add_resource(store_sales, '/store_sales/<int:store_id>')

if __name__ == '__main__':
    app.run(debug=True)
