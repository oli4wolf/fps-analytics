import requests
import logging

from datetime import datetime, timedelta

from regexFilter import extract_data

def main(auth_token: str, days: int):
    base_url = "https://sbb.splunkcloud.com:8089/services/search/jobs/export/"

    # Get current date
    current_date = datetime.now()
    
    # Loop through last X days
    for i in range(days):
        # Create the call
        datePlus = current_date - timedelta(days=i)
        date = current_date - timedelta(days=i+1)
        head = {'Authorization': 'Bearer {}'.format(auth_token)} #Authorization: Bearer
        url = f'{base_url}?output_mode=csv&search=search index=sbb_journey-service_internal_prod_events "Analytics" &earliest_time={date.year}-{date.month}-{date.day}T00%3A00%3A00.000-02%3A00&latest_time={datePlus.year}-{datePlus.month}-{datePlus.day}T00%3A00%3A00.000-02%3A00&loglevel=INFO'
        logging.info(url)
        output_filename = f'./download/output{date.year}{date.month}{date.day}'
        response = requests.get(url, headers=head)

        # Evaluate the Response
        if response.status_code == 200:
            with open(f'{output_filename}.csv', 'w', encoding='utf-8') as f:
                for line in response.text.splitlines():
                    line = extract_data(line)
                    if line != None:
                        f.write(line + '\n')
        #Handle error but log it.
        else:
            logging.error(f"Error: Status code {response.status_code}")
            logging.error(response.text)
            with open(f'{output_filename}-Error-{response.status_code}.txt', 'w', encoding='utf-8') as f:
                f.write(response.text)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1],1)
    elif len(sys.argv) > 2:
        main(sys.argv[1],sys.argv[2])
    else:
        print("Please provide auth_token, optional: days")
