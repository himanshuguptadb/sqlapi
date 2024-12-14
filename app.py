from flask import Flask, request
from flask_restful import Api, Resource

app = Flask(__name__)
api = Api(app)

# Sample data
books = [
    {"id": 1, "title": "Python Basics", "author": "John Doe"},
    {"id": 2, "title": "Flask Web Development", "author": "Jane Smith"}
]

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

if __name__ == '__main__':
    app.run(debug=True)
