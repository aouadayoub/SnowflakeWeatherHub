import json

# http://api.weatherapi.com/v1/current.json?key=&q=Morocco&aqi=no
from datetime import datetime
import requests  
import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("weather_table")


def get_weather_data(city):  
    api_url = "http://api.weatherapi.com/v1/current.json"
    params = {  
        "q": city,    
        "key": "YOUR_API_KEY"
    }  
    response = requests.get(api_url, params=params)  
    data = response.json()  
    return data  
    
    

def lambda_handler(event, context):

    cities = ["Casablanca", "Rabat", "Marrakech", "Fes", "Tangier", "Agadir", "Oujda", "Meknes", "Essaouira", "Tiznit"]

    for city in cities:
        data = get_weather_data(city)  
    
        temp = data['current']['temp_c']
        wind_speed = data['current']['wind_mph']
        wind_dir = data['current']['wind_dir']
        feelslike_c =  data['current']['feelslike_c']
        pressure_mb = data['current']['pressure_mb']
        humidity = data['current']['humidity']
        visibility_km = data['current']['vis_km']
    
        print(city,temp,wind_speed,wind_dir,pressure_mb,humidity)
        current_timestamp = datetime.utcnow().isoformat()
        
        item = {
                'city': city,
                'time': str(current_timestamp),
                'temp': temp,
                'wind_speed': wind_speed,
                'wind_dir': wind_dir,
                'pressure_mb': pressure_mb,
                'humidity': humidity,
                'visibility_km': visibility_km,
                'feelslike_c': feelslike_c

            }
        item = json.loads(json.dumps(item), parse_float=Decimal)
        table.put_item(
            Item=item
        )
    

 