import os
import pandas as pd
from multiprocessing import Pool
import re
import json

'''
log.info("Analytics#{}#{}#{}#{}#{}#{}#{}#{}#{}",
                    requestContext.getClientId(),
                    origin != null ? origin.getPlaceId() : "",
                    destination != null ? destination.getPlaceId() : "",
                    vias != null ? vias.stream().map(this::mapToStopPlaceId).collect(Collectors.toList()) : List.of(),
                    dateTime != null ? toLocalDateTimeWithoutSeconds(dateTime.toLocalDateTime()) : "",
                    toLocalDateTimeWithoutSeconds(requestContext.getRequestTime()),
                    getScrollDirection(paginationCursor),
                    requestContext.getApiPathPattern(),
                    tripAnalyzer.getRouteProspection(tripResponse));

timestamp=2024-10-11 03:59:59,608 loglevel=INFO  class=c.s.k.j.b.v.a.t.o.TripObserver client_id=- request_id=- trace_id=- message=Analytics#81415646-bde8-4b31-ac85-5a4c6aaf0d2f#[8.7223991,47.5095506]#8503016#[]#2024-10-11T05:24#2024-10-11T03:59#B#/v3/trips/by-origin-destination#{"8506000|8503016":2,"8573692|8506000|8503016":1}
'''
# Define column names based on the log structure
columns = [
        'client_id',
        'origin_place_id',
        'destination_place_id',
        'via_stops',
        'date_time',
        'request_time',
        'scroll_direction',
        'api_path_pattern',
        'route_prospection'
]
fps_analytics_dict = {}

def process_file(file_path):
    results = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        for line in file:
            match = re.search(r'Analytics#(.+?)(?:\s|$)', line)
            if match:
                results.append(match.group(1).split('#'))
            legs_dict = json.loads(str(results[7]).replace("'", '"'))
            for key, value in legs_dict.items():
                fps_analytics_dict.keys[]
            fps_analytics_dict.append(legs_dict) #Add to dictionary with the count.
    dd = pd.DataFrame(results)
    dd.to_csv(file.name+'_results.csv', index=False)
    # {"8503000|8503206|8503283":4,"8503000|8503206|8503286|8503283":1}

    return dd

def read_data_files():
    data_folder = 'data'
    # Get list of all file paths
    file_paths = [os.path.join(data_folder, f) for f in os.listdir(data_folder) 
                 if os.path.isfile(os.path.join(data_folder, f))]
    
    # Create pool and map files to processes
    with Pool() as pool:
        dfs = pool.map(process_file, file_paths)
    
    # Combine all dataframes
    return pd.concat(dfs, ignore_index=True)

def main():
    fps_analytics_df = pd.DataFrame(columns=columns)

    # Read all data files
    fps_analytics_df = read_data_files()
    
    # Display results
    print("Analytics DataFrame:")
    print(fps_analytics_df.head())
    print(f"\nTotal records: {len(fps_analytics_df)}")

    # Save to CSV
    fps_analytics_df.to_csv('fps_analytics_results.csv', index=False)

if __name__ == "__main__":
    main()