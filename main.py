import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

BASE_URL = 'https://raw.githubusercontent.com/brightcove/streaming-dataset/main/datasets/'
HEADER_LIST = ['session', 'seq', 'device_type', 'device_os', 'browser', 
              'player', 'player_width', 'player_height', 
              'rendition_indicated_bps', 'rendition_width', 
              'rendition_height', 'rendition_framerate', 'video_codec', 
              'video_codec_profile', 'format', 'segment_duration', 
              'video_seconds_viewed', 'forward_buffer_seconds', 
              'rebuffering_seconds', 'rebuffering_count', 
              'media_bytes_transferred', 'measured_bps']

# Number of files to pull for each event. Can reduce if you want to test
EVENT_1 = 30
EVENT_2 = 2 #13

def plot_sessions(df: pd.DataFrame):
    sns.histplot(data=df, y='seq_s', hue='session')
    plt.show()

def get_session_info(df: pd.DataFrame):
    unique_sessions = df['session'].nunique()
    print(f'Number of unique sessions: {unique_sessions}')
    max_session_length = df['session'].max()
    print(f'Max session length: {max_session_length}')

    session_df = df[['session', 'seq']]
    # each seq number corresponds to 10 seconds that is sent from the player
    session_df['seq'] = df['seq'].apply(lambda x: x * 10)
    session_df.rename(columns={"seq": "seq_s"}, inplace=True)
    
    # TODO: drop sessions with duration less than 50 seqs
    plot_sessions(session_df)

def fetch_data(event_number: str) -> pd.DataFrame:
    NUM_FILES = EVENT_1 if event_number == 1 else EVENT_2
    for i in range(NUM_FILES):
        print(f'Reading csv file {i} ...')
        skiprows = 1 if i == 0 else 0
        df = pd.concat([pd.read_csv(f'{BASE_URL}event{event_number}/event{event_number}_playback_statistics_{i}.csv', 
        names=HEADER_LIST, skiprows=skiprows)], ignore_index=True)
    return df


if __name__ == "__main__":
    df = fetch_data(2)
    get_session_info(df)
    