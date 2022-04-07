import pandas as pd

BASE_URL = 'https://raw.githubusercontent.com/brightcove/streaming-dataset/main/datasets/'
HEADER_LIST = ['session', 'seq', 'device_type', 'device_os', 'browser', 
              'player', 'player_width', 'player_height', 
              'rendition_indicated_bps', 'rendition_width', 
              'rendition_height', 'rendition_framerate', 'video_codec', 
              'video_codec_profile', 'format', 'segment_duration', 
              'video_seconds_viewed', 'forward_buffer_seconds', 
              'rebuffering_seconds', 'rebuffering_count', 
              'media_bytes_transferred', 'measured_bps']
EVENT_1 = 30
EVENT_2 = 13

def fetch_data(event_number: str) -> pd.DataFrame:
    NUM_FILES = EVENT_1 if event_number == 1 else EVENT_2
    for i in range(NUM_FILES):
        print(f'Reading csv file {i} ...')
        skiprows = 1 if i == 0 else 0
        df = pd.concat([pd.read_csv(f'{BASE_URL}event{event_number}/event{event_number}_playback_statistics_{i}.csv', 
        names=HEADER_LIST, skiprows=skiprows)], ignore_index=True)
    return df

def get_session_length(df):
    # get session duration based on seq
    # plot cdf of session duration
    pass


if __name__ == "__main__":
    df = fetch_data(2)
