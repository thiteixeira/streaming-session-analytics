import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

sns.set_theme(style="whitegrid")

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
EVENT_2 = 13

def plot_sessions(df: pd.DataFrame):
    ax = sns.scatterplot(x='seq', y='session', data=df, hue='session', legend=None)
    ax.set_title('Distribution of Session Lengths')
    ax.set(xlabel='Session Duration (s)', ylabel='Session ID')
    plt.legend(title='Event 2', loc='upper right')
    plt.show()

def get_session_info(df: pd.DataFrame):
    unique_sessions = df['session'].nunique()
    print(f'Number of unique sessions: {unique_sessions}')
    max_session_length = df['session'].max()
    print(f'Max session length: {max_session_length}')

    session_df = df[['session', 'seq']]
    # Find the max seq number for each session
    session_df = session_df.loc[df.groupby('session')['seq'].idxmax()]
    session_df.reset_index(inplace=True)

    # Drop rows where seq is less than 300 (50 percentile)
    session_df.drop(session_df[session_df.seq < 300].index, inplace=True)

    # each seq number corresponds to a 10-second beacon sent from the player
    session_df['seq'] = df['seq'].apply(lambda x: x * 10)
    print(session_df)

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
    # TODO: add argparse to allow user to specify event number
    df = fetch_data(2)
    get_session_info(df)
    