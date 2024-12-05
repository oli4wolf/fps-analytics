import os
import shutil
import pandas as pd
import logging
from multiprocessing import Pool

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
            results.append(line.split('#'))
    dd = pd.DataFrame(results)
    # change file path to data for the results dirty.
    filename = file.name.replace('download', 'data')
    dd.to_csv(f'{filename}_pandas.csv', index=False)
    return dd

def read_data_files():
    data_folder = 'download'
    # Get list of all csv file paths
    file_paths = [os.path.join(data_folder, f) for f in os.listdir(data_folder) 
                 if os.path.isfile(os.path.join(data_folder, f)) and f.endswith('.csv')]    
    # Create pool and map files to processes
    with Pool() as pool:
        dfs = pool.map(process_file, file_paths)
    
    # Combine all dataframes
    return pd.concat(dfs, ignore_index=True)

def main():
    fps_analytics_df = pd.DataFrame(columns=columns)

    # Delete the content in folder data before running this script.
    if os.path.exists('data'):
        shutil.rmtree('data')
    os.makedirs('data', exist_ok=True)

    # Read all data files
    fps_analytics_df = read_data_files()
    
    # Display results
    logging.info("Analytics DataFrame:")
    logging.info(fps_analytics_df.head())
    logging.info(f"\nTotal records: {len(fps_analytics_df)}")

    # Save to CSV
    fps_analytics_df.to_csv('./data/fps_analytics_pandas.csv', index=False)

if __name__ == "__main__":
    main()