import json,re
import pandas as pd
import numpy as np
import boto3
import json

def load_json_from_s3(s3_client, bucket_name, key,ndjson:bool=False):
    """
    Reads a JSON file from S3 and returns it as a Python object.

    Parameters:
        bucket_name (str): Name of the S3 bucket
        key (str): Path/key to the JSON file (e.g., 'data/file.json')
        aws_conn_id (str): Optional Airflow connection ID (if using Airflow context)

    Returns:
        dict or list: Parsed JSON content
    """


    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    content = response['Body'].read().decode('utf-8')

    if ndjson:
        return [json.loads(line) for line in content.splitlines()]
    
    return json.loads(content)



def convert_to_list_dicts(data) -> list:
    """
    Converts nested auction data to flat dictionaries, handling special list fields.
    
    Args:
        data: Input auction data (dict or list format)
            - If dict: {url: {auction_data}}
            - If list: [{auction_data}]
            
    Returns:
        list: Flattened auction records with consistent structure
    """
    
    def extract_list_field(field_data, list_key=None):
        """Extracts list from field data with flexible key handling"""
        if field_data is None:
            return []
        if isinstance(field_data, list):
            return field_data
        if isinstance(field_data, dict):
            if list_key:
                return field_data.get(list_key, [])
        return []


    def process_auction(auction, url=None):
        """Process individual auction record"""

        auction_stats = auction.get('auction_stats', {}) or {}
        auction_stats.setdefault('view_count', 0)
        auction_stats.setdefault('watcher_count', 0)

        auction_data = {
            "auction_url": url if url else auction.get('auction_url'),
            "auction_title": auction.get('auction_title'),
            "auction_subtitle": auction.get('auction_subtitle'),
            "dougs_take": auction.get('dougs_take'),
            "auction_highlights": extract_list_field(
                auction.get('auction_highlights'), 
                list_key='bullet_points'
            ),
            "services": extract_list_field(
                auction.get('services') or auction.get('service_history'),
                list_key='items'
            ),
            "auction_equipment": auction.get('auction_equipment'),
            "modifications": auction.get('modifications'),
            "known_flaws": auction.get('known_flaws'),
            "included_items": auction.get('included_items'),
            "ownership_history": auction.get('ownership_history'),
            "seller_notes": auction.get('seller_notes'),
            "auction_videos":auction.get('auction_videos',[]),
            **auction.get('auction_quick_facts', {}),
            **auction_stats,
        }
        return auction_data


    # Handle input types
    if isinstance(data, dict):
        return [process_auction(data[auction], auction) for auction in data]
    if isinstance(data, list):
        return [process_auction(auction) for auction in data]
    raise ValueError("Input must be dictionary or list")



def create_auction_df(auctions:list):
    df = pd.DataFrame(auctions)
    print(df.head(10))
    df.columns = df.columns.str.lower().str.replace(" ","_")
    return df



def get_and_remove_invalid_auctions(df):
    """
    Identifies auctions with invalid auction_status values by checking for key phrases.
    Removes them from the DataFrame and returns their URLs for re-scraping.
    
    Parameters:
        df (pd.DataFrame): The auction DataFrame.
    
    Returns:
        tuple: (cleaned DataFrame, list of auction URLs with invalid statuses)
    """
    # Find auctions with valid auction status
    valid_status_mask = (
        df['auction_status'].str.lower().str.contains('sold|reserve not met|cancelled', na=False)
    )
    
    # Get URLs of invalid auctions
    rescrape_urls = df.loc[~valid_status_mask, 'auction_url'].tolist()
    
    # Keep only valid auctions
    clean_auctions_df = df.loc[valid_status_mask].copy()
    
    return clean_auctions_df, rescrape_urls


def clean_and_transform(df):

    # Convert 'auction_date' to datetime
    df['auction_date'] = pd.to_datetime(df['auction_date'],utc=True)
    df = df.sort_values('auction_date', ascending=False).reset_index(drop=True)

    # extract auction id
    def extract_auction_id(url:str)->str:
        return url.strip().split("/")[4]

    df['auction_id'] = df['auction_url'].apply(extract_auction_id)


    # drop duplicates based on auction id
    df = df.drop_duplicates('auction_id', keep='first')

    # clean 'model'
    df['model'] = df['model'].str.split('\n').str[0].str.strip()


    # Convert 'mileage' to integer
    def extract_mileage(value):
        if pd.isna(value):
            return None
        match = re.search(r'[\d,]+',value)
        if match:
            return int(match.group(0).replace(',',''))
        return None
    
    df['mileage'] = df['mileage'].apply(extract_mileage) 


    # convert 'highest-bid_value' to float
    df['highest_bid_value'] = df['highest_bid_value'].str.replace('$','').str.replace(',','').astype(float)

    # Convert 'bid_count' to integer
    df['bid_count'] = pd.to_numeric(df['bid_count'], errors='coerce')

    # Convert 'view_count' to integer
    df['view_count'] = df['view_count'].astype(str).str.replace(',', '', regex=False)
    df['view_count'] = pd.to_numeric(df['view_count'], errors='coerce').fillna(0).astype(int)

    # Convert 'watcher_count' to integer
    df['watcher_count'] = df['watcher_count'].astype('str').replace(',','', regex=True)
    df['watcher_count'] = pd.to_numeric(df['watcher_count'], errors="coerce").fillna(0).astype(int)

    # clean 'auction status' - change 'sold to' to 'sold'
    df['auction_status'] = df['auction_status'].str.replace('Sold to','Sold').replace('Reserve not met, bid to', 'Reserve not met')    
    
    # Create boolean col for reserve status
    df['reserve_met'] = df['auction_status'].str.lower().eq('sold')

    # remove 'follow' from seller
    df['seller'] = df['seller'].str.split('\n').str[0].str.strip()

    # clean bids
    def clean_bids(bids_list):
        try:
            return [int(bid.replace("$",'').replace(',','')) for bid in bids_list]
        except Exception as e:
            return []
        
    df['bids'] = df['bids'].apply(clean_bids)


    # Split 'title_status' into 'title_status_clean' and 'title_state'
    df['title_status_cleaned'] = df['title_status'].str.extract(r'^(.*?) \(')
    df['title_state'] = df['title_status'].str.extract(r'\((.*?)\)')


    # Split 'location' into 'city' and 'state'
    def extract_city_state(location):
            if pd.isna(location):
                return None, None
            try:
                parts = location.rsplit(",", 1)
                if len(parts) == 2:
                    city = parts[0].strip()
                    state = parts[1].strip().split(" ")[0]
                    return city,state
                else:
                    return parts[0].strip(), None
            except Exception:
                raise

    df[['city','state']] = df['location'].apply(extract_city_state).apply(pd.Series)


    # clean transmission
    def clean_transmission(trans_str):
        if not trans_str or not isinstance(trans_str, str):
            return None, None

        trans_str = trans_str.lower()
        transmission_type = 'Other'
        if 'manual' in trans_str:
            transmission_type = 'Manual' 
        elif 'auto' in trans_str:
            transmission_type = 'Automatic'

        match = re.search(r'(\d+)-speed', trans_str)
        gears = int(match.group(1)) if match else None

        return transmission_type, gears

    df['transmission_type'], df['gears'] = zip(*df['transmission'].map(clean_transmission))
    df['transmission_type'] = df['transmission_type'].astype('object') 

    # clean drivetrain
    def clean_drivetrain(drive_str):
        if not drive_str or not isinstance(drive_str, str):
            return 'Other'

        drive_str = drive_str.lower()

        if '4wd' in drive_str and 'awd' in drive_str:
            return '4WD/AWD'
        elif 'front' in drive_str:
            return 'FWD'
        elif 'rear' in drive_str:
            return 'RWD'
        elif 'awd' in drive_str or 'all-wheel' in drive_str:
            return 'AWD'
        elif '4wd' in drive_str or 'four-wheel' in drive_str:
            return '4WD'
        else:
            return 'Other'
        
    df['drivetrain'] = df['drivetrain'].apply(clean_drivetrain)

    # extract bids features
    def extract_bid_features(bids_list):
        if not bids_list or not isinstance(bids_list, list) or len(bids_list) < 2:
            return pd.Series(
                {
                    'max_bid': np.nan,
                    'min_bid': np.nan,
                    'mean_bid': np.nan,
                    'median_bid': np.nan,
                    'bid_range': np.nan,
                }
            )

        return pd.Series(
            {
                'max_bid': max(bids_list),
                'min_bid': min(bids_list),
                'mean_bid': np.mean(bids_list),
                'median_bid': np.median(bids_list),
                'bid_range': max(bids_list) - min(bids_list),
            }
        )

    features_df = df['bids'].apply(extract_bid_features)
    df = df.join(features_df)

    # add count fields for auction flaws, services, equipment, extra items, highlights,
    def count_list(x):
        return len(x) if isinstance(x, list) else None

    df['highlight_count'] = df['auction_highlights'].apply(count_list)
    df['equipment_count'] = df['auction_equipment'].apply(count_list)
    df['mod_count'] = df['modifications'].apply(count_list)
    df['flaw_count'] = df['known_flaws'].apply(count_list)
    df['service_count'] = df['services'].apply(count_list)
    df['included_items_count'] = df['included_items'].apply(count_list)
    df['video_count'] = df['auction_videos'].apply(count_list)

    
    # extract manufacture year
    def extract_manufacture_year(url):
        if pd.isna(url):
            return None
        try:
            return int(url.strip().split("/")[-1].split("-")[0])
        except Exception:
            None

    df['manufacture_year'] = df['auction_url'].apply(extract_manufacture_year)

    return df




