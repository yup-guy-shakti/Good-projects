from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, validator
from typing import List, Optional
from geopy.distance import geodesic
import sqlite3

# Define the Address data model using Pydantic BaseModel


class Address(BaseModel):
    id: Optional[int]
    street: str
    city: str
    state: str
    zip: str
    latitude: float
    longitude: float

# Use a validator to ensure that zip codes are valid
    @validator('zip')
    def validate_zip(cls, v):
        if not v.isdigit() or len(v) != 5:
            raise ValueError('Invalid zip code')
        return v


# Create a new FastAPI instance
app = FastAPI()

# Create a route to handle POST requests to create new addresses


@app.post('/addresses/')
def create_address(address: Address):
    with sqlite3.connect('addresses.db') as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO addresses (street, city, state, zip, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?)',
                       (address.street, address.city, address.state, address.zip, address.latitude, address.longitude))
        address.id = cursor.lastrowid
        conn.commit()
    return address

# Create a route to handle PUT requests to update existing addresses


@app.put('/addresses/{address_id}')
def update_address(address_id: int, address: Address):
    with sqlite3.connect('addresses.db') as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE addresses SET street = ?, city = ?, state = ?, zip = ?, latitude = ?, longitude = ? WHERE id = ?',
                       (address.street, address.city, address.state, address.zip, address.latitude, address.longitude, address_id))
        if cursor.rowcount < 1:
            raise HTTPException(status_code=404, detail='Address not found')
        conn.commit()
    address.id = address_id
    return address

# Create a route to handle DELETE requests to delete existing addresses


@app.delete('/addresses/{address_id}')
def delete_address(address_id: int):
    with sqlite3.connect('addresses.db') as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM addresses WHERE id = ?', (address_id,))
        if cursor.rowcount < 1:
            raise HTTPException(status_code=404, detail='Address not found')
        conn.commit()
    return {'message': 'Address deleted'}

# Create a route to handle GET requests for nearby addresses


@app.get('/addresses/nearby')
def get_nearby_addresses(latitude: float, longitude: float, distance: float):
    with sqlite3.connect('addresses.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT id, street, city, state, zip, latitude, longitude FROM addresses')
        rows = cursor.fetchall()
        addresses = []
        for row in rows:
            if geodesic((row[5], row[6]), (latitude, longitude)).miles <= distance:
                address = dict(
                    zip(('id', 'street', 'city', 'state', 'zip', 'latitude', 'longitude'), row))
                addresses.append(address)
        return addresses
