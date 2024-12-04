import requests
import concurrent.futures

from datetime import datetime, timedelta

from regexFilter import extract_data

#Unused function
def run_subprocess_request(url: str, headers: dict, output_file: str) -> None:
    try:
        # Using concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future = executor.submit(requests.get, url, headers=headers)
            response = future.result()
            
            if response.status_code == 200:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
            else:
                print(f"Error: Status code {response.status_code}")
                with open(f'{output_file}-Error-{response.status_code}.txt', 'w', encoding='utf-8') as f:
                    f.write(response.text)
                    
        return response
    except concurrent.futures.TimeoutError:
        print("Request timed out")
        return None
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return None

def main(auth_token: str):
    base_url = "https://sbb.splunkcloud.com:8089/services/search/jobs/export/"


    # Get current date
    current_date = datetime.now()
    
    # Loop through last 30 days
    for i in range(5):
        datePlus = current_date - timedelta(days=i)
        date = current_date - timedelta(days=i+1)
        head = {'Authorization': 'Bearer {}'.format(auth_token)} #Authorization: Bearer
        url = f'{base_url}?output_mode=csv&search=search index=sbb_journey-service_internal_prod_events "Analytics" &earliest_time={date.year}-{date.month}-{date.day}T00%3A00%3A00.000-02%3A00&latest_time={datePlus.year}-{datePlus.month}-{datePlus.day}T00%3A00%3A00.000-02%3A00&loglevel=INFO'
        print(url)
        output_filename = f'./download/output{date.year}{date.month}{date.day}'
        response = requests.get(url, headers=head)

        if response.status_code == 200:
            with open(f'{output_filename}.csv', 'w', encoding='utf-8') as f:
                for line in response.text.splitlines():
                    line = extract_data(line)
                    if line != None:
                        f.write(line + '\n')
        else:
            print(f"Error: Status code {response.status_code}")
            print(response.text)
            with open(f'{output_filename}-Error-{response.status_code}.txt', 'w', encoding='utf-8') as f:
                f.write(response.text)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Please provide auth_token")
