import os
from polygon import RESTClient
import pandas as pd
import requests

def save_to_csv(data, filename):
    try:
        # Rename columns with full forms
        column_mapping = {
            'v': 'Volume',
            'vw': 'Volume Weighted Average Price',
            'o': 'Open',
            'c': 'Close',
            'h': 'High',
            'l': 'Low',
            'n': 'Number of Transactions'
        }
        data.rename(columns=column_mapping, inplace=True)

        # Append to CSV
        mode = 'a' if os.path.exists(filename) else 'w'
        data.to_csv(filename, mode=mode, index=False, header=mode=='w')
        print(f"Data appended to {filename} successfully!")
    except Exception as e:
        print(f"Error saving data: {e}")


# Function to get historic stock data
def get_historic_stock_data(symbol, start_date, end_date, api_key):
    base_url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}?unadjusted=false&apiKey={api_key}'
    response = requests.get(base_url)
    if response.status_code == 200:
        try:
            data = response.json()
            if 'results' in data:
                results = data['results']
                if results:
                    df = pd.DataFrame(results)
                    # Convert timestamp to datetime format
                    if 't' in df.columns:
                        # Convert timestamp to datetime format
                        df['Datetime'] = pd.to_datetime(df['t'], unit='ms')
                        # Separate date and time into two different columns
                        # df['Date'] = df['Datetime'].dt.date
                        # df['Time'] = df['Datetime'].dt.time
                        # Add Symbol column
                        df['Symbol'] = symbol
                        # Reorder columns
                        df = df[['Symbol', 'Datetime', 'o', 'h','l','c','v', 'vw', 'n']]
                        # Rename columns
                        column_mapping = {
                            'v': 'Volume',
                            'vw': 'Volume Weighted Average Price',
                            'o': 'Open',
                            'c': 'Close',
                            'h': 'High',
                            'l': 'Low',
                            'n': 'Number of Transactions'
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
        except ValueError as e:
            print("Error parsing JSON:", e)
            print("Response content:", response.content)
            return None
    else:
        print("Error:", response.status_code, response.reason)
        return None


# Polygon API key
polygonAPIkey = "API_KEY"
client = RESTClient(polygonAPIkey)

# Get exchanges
exchanges = pd.DataFrame(client.get_exchanges(asset_class='stocks', locale='us'))

exchangeList = list(set(exchanges.mic))
exchangeList.remove(None)

# Get all tickers from all US exchanges
usTickers = []
for x in exchangeList:
    for t in client.list_tickers(market='stocks', exchange=x, active=False, limit=1000):
        usTickers.append(t.ticker)

# Final ticker list
finalTickerList = set(usTickers)
print(f"Total number of Tickers {len(finalTickerList)}")
# Define start and end dates
start_date = "1990-01-01"  # Example start date
end_date = "2024-04-16"  # Example end date

# Set the output filename
output_filename = "final_all_tickers_historic_data.csv"

# Iterate over each ticker to fetch data
for symbol in finalTickerList:
    stock_data = get_historic_stock_data(symbol, start_date, end_date, polygonAPIkey)
    if stock_data is not None:
        save_to_csv(stock_data, output_filename)
        print(f"Data for {symbol} saved successfully!")
    else:
        print(f"No data available for {symbol}.")

print(f"All data saved to {output_filename} successfully!")
