import os
from polygon import RESTClient
import pandas as pd
import requests


# Function to get historic stock data
def get_historic_stock_data(symbol, start_date, end_date, api_key):
    base_url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}?unadjusted=false&apiKey={api_key}'
    try:
        response = requests.get(base_url)
        response.raise_for_status()  # Raise an error for bad responses (status codes other than 200)
        data = response.json()
        if 'results' in data:
            results = data['results']
            if results:
                df = pd.DataFrame(results)
                # Convert timestamp to datetime format
                if 't' in df.columns:
                    # df['Datetime'] = pd.to_datetime(df['t'], unit='ms')
                    # # Convert to Eastern Time
                    # df['Datetime'] = df['Datetime'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

                    df['Datetime'] = pd.to_datetime(df['t'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(
                        'US/Eastern')

                    # Extract only the datetime portion without timezone offset
                    df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

                    df['Symbol'] = symbol
                    # Reorder columns
                    df = df[['Symbol', 'Datetime', 'o', 'h', 'l', 'c', 'v']]
                    # Rename columns
                    column_mapping = {
                        'v': 'Volume',
                        'o': 'Open',
                        'c': 'Close',
                        'h': 'High',
                        'l': 'Low'
                    }
                    df.rename(columns=column_mapping, inplace=True)
                    # Drop unnecessary columns
                else:
                    print(f"Timestamp column not found in data for {symbol}")
                    return None
                return df
            else:
                print(f"No historical data available for {symbol}")
                return None
        else:
            print("Error: 'results' key not found in data:", data)
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data for {symbol}: {e}")
        return None
    except ValueError as e:
        print(f"Error parsing JSON response for {symbol}: {e}")
        print("Response content:", response.content)
        return None


# Function to save data to CSV
def save_to_csv(data, start_date, end_date, symbol, directory="New_Stock_data"):
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = os.path.join(directory, f"{symbol}_{start_date}_to_{end_date}_data.csv")

    # Rename columns with full forms
    column_mapping = {
        'v': 'Volume',
        'o': 'Open',
        'c': 'Close',
        'h': 'High',
        'l': 'Low'
    }
    data.rename(columns=column_mapping, inplace=True)

    # Save to CSV
    try:
        data.to_csv(filename, index=False)
        print(f"Data for {symbol} saved successfully!")
    except Exception as e:
        print(f"An error occurred while saving data for {symbol}: {e}")


# Polygon API key
polygonAPIkey = "API_KEY"
client = RESTClient(polygonAPIkey)

# Get exchanges
exchanges = pd.DataFrame(client.get_exchanges(asset_class='stocks', locale='us'))

exchangeList = list(set(exchanges.mic))
exchangeList.remove(None)

# All tickers list
usTickers = ['AMD','META']
# Final ticker list
finalTickerList = set(usTickers)
print(f"Total number of Tickers {len(finalTickerList)}")
# Define start and end dates
start_date = "2024-01-01"  # Example start date
end_date = "2024-04-16"  # Example end date

# Iterate over each ticker to fetch and save data
for symbol in finalTickerList:
    stock_data = get_historic_stock_data(symbol, start_date, end_date, polygonAPIkey)
    if stock_data is not None:
        save_to_csv(stock_data, start_date, end_date, symbol)
    else:
        print(f"No data available for {symbol}.")
