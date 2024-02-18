from flask import Flask, jsonify,request
from pymongo import MongoClient
from urllib.parse import quote_plus
import pandas as pd


username = "assignment"
password = "password@123"

# URL-encode the username and password
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

# Create the MongoDB connection string
connection_string = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.zosqclv.mongodb.net/?retryWrites=true&w=majority"

# Use the connection string in MongoClient


app = Flask(__name__)


# MongoDB connection configuration
client = MongoClient(connection_string)
db = client['amazon']  # Replace 'your_database_name' with your actual database name
collection = db['products']  # Replace 'your_collection_name' with your actual collection name

@app.route('/', methods=['GET'])
def hello():
    return "Healthy"

@app.route('/products', methods=['GET'])
def get_data():
    try:
        # Get query parameters for pagination
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 10))

        # Calculate skip value based on page and page_size
        skip = (page - 1) * page_size

        # Retrieve documents from the collection with pagination
        data = list(collection.find({}, {'_id': 0}).skip(skip).limit(page_size))

        return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"Error retrieving data from MongoDB: {e}"}), 500

@app.route('/products/<int:product_id>', methods=['GET'])
def get_product_by_id(product_id):
    try:
        # Retrieve a specific document from the collection based on product_id
        data = collection.find_one({'code': product_id}, {'_id': 0})

        if data:
            return jsonify(data)
        else:
            return jsonify({"error": "Product not found"}), 404
    except Exception as e:
        print(f"Error fetching product from MongoDB: {e}")

@app.route('/products', methods=['POST'])
def create_product():
    try:
        # Extract product data from the request JSON
        product_data = request.get_json()
        print(product_data)

        # Insert the product data into the MongoDB collection
        result = collection.insert_one(product_data)

        # Check if the insertion was successful
        if result.inserted_id:
            return jsonify({"message": "Product created successfully", "product_id": str(result.inserted_id)}), 201
        else:
            return jsonify({"error": "Failed to create product"}), 500
    except Exception as e:
        return jsonify({"error": f"Error creating product in MongoDB: {e}"}), 500
    # Handle the exception appropriately, such as returning an error response to the client


@app.route('/products/<int:product_code>', methods=['PUT'])
def update_product(product_code):
    try:
        # Extract updated product data from the request JSON
        updated_data = request.get_json()

        # Update the product based on the product code
        result = collection.replace_one({'code': product_code}, updated_data)

        # Check if the update was successful
        if result.modified_count > 0:
            return jsonify({"message": "Product updated successfully"}), 200
        else:
            return jsonify({"error": "Product not found or update failed"}), 404
    except Exception as e:
        return jsonify({"error": f"Error updating product in MongoDB: {e}"}), 500

@app.route('/products/<int:product_code>', methods=['DELETE'])
def delete_product(product_code):
    try:
        # Delete the product based on the product code
        result = collection.delete_one({'code': product_code})

        # Check if the deletion was successful
        if result.deleted_count > 0:
            return jsonify({"message": "Product deleted successfully"}), 200
        else:
            return jsonify({"error": "Product not found or delete failed"}), 404
    except Exception as e:
        print(f"Error deleting product from MongoDB: {e}")
        # Handle the exception appropriately, such as returning an error response to the client


# @app.route('/delete', methods=['GET'])
# def delete_few_products():
#     ids=list(range(100000, 300000))
#     result = collection.delete_many({'code': {'$in': ids}})
#     print("deleted ",result)
#     return "deleted"

# @app.route('/import-csv', methods=['GET'])
# def import_csv_from_local():
#     file_path='./Amazon-Products.csv'
#     a=pd.read_csv(file_path)
#     data = a.to_dict(orient='records')
#     # logic to import file
#     collection.insert_many(data)
#     return "Imported successfully"

if __name__ == '__main__':
    app.run(debug=True)
