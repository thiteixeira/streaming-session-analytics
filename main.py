#!.venv/bin/python

import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

sns.set_theme(style="whitegrid")

EVENT_1 = 38
EVENT_2 = 12
EVENT_3 = 1
EVENT_4 = 4
BASE_URL = (
    "https://raw.githubusercontent.com/brightcove/streaming-dataset/main/datasets/"
)
HEADER_LIST = [
    "session",
    "seq",
    "device_type",
    "device_os",
    "browser",
    "player",
    "player_width",
    "player_height",
    "rendition_indicated_bps",
    "rendition_width",
    "rendition_height",
    "rendition_framerate",
    "video_codec",
    "video_codec_profile",
    "format",
    "segment_duration",
    "video_seconds_viewed",
    "forward_buffer_seconds",
    "rebuffering_seconds",
    "rebuffering_count",
    "media_bytes_transferred",
    "measured_bps",
]


def load_data(num_of_files: int, event_num: int = 1) -> pd.DataFrame:
    """Fetch data from GitHub repo and load it to a dataframe."""

    if num_of_files == None or event_num == 3:
        if event_num == 1:
            num_of_files = EVENT_1
        elif event_num == 2:
            num_of_files = EVENT_2
        elif event_num == 3:
            num_of_files = EVENT_3
        elif event_num == 4:
            num_of_files = EVENT_4

    dfs = []
    for i in range(num_of_files):
        print(f"Reading csv file {i} ...")
        url = (
            f"{BASE_URL}event{event_num}/event{event_num}_playback_statistics_{i}.csv"
            if event_num != 3
            else f"{BASE_URL}event{event_num}/event{event_num}_playback_statistics.csv"
        )
        if i == 0:
            dfs.append(pd.read_csv(url))
        else:
            skiprows = 1 if i == 0 else 0
            dfs.append(
                pd.read_csv(
                    url,
                    names=HEADER_LIST,
                    skiprows=skiprows,
                )
            )
    return pd.concat(dfs)


def analyze_bitrate(event_df: pd.DataFrame) -> None:
    """Compute the CDF of the measured bitrate."""

    print("Analyzing bitrate ...")
    df = (
        event_df[["seq", "rendition_indicated_bps"]]
        .groupby("seq", as_index=False)["rendition_indicated_bps"]
        .mean()
    )
    df["rendition_indicated_kbps"] = df["rendition_indicated_bps"] / 1000

    ax = sns.ecdfplot(data=df, x="rendition_indicated_kbps", legend=None)
    ax.set_title("CDF of Average bitrate")
    ax.set(xlabel="Average Rendition Bitrate (kbps)", ylabel="CDF")
    plt.savefig("./assets/cdf_bitrate.png")
    plt.show()


def analyze_fluctuation(event_df: pd.DataFrame) -> None:
    pass


def analyze_rate_rebuffering(event_df: pd.DataFrame) -> None:
    pass


def analyze_buffering_ratio(event_df: pd.DataFrame) -> None:
    pass


if __name__ == "__main__":
    event_parser = argparse.ArgumentParser(
        description="Define event and number of files"
    )
    event_parser.add_argument(
        "--evt-num",
        dest="event_number",
        type=int,
        choices=[1, 2, 3, 4],
        help="event number",
    )
    event_parser.add_argument(
        "--num-files",
        dest="num_files",
        type=int,
        help="number of files to read from GitHub",
    )
    event_parser.add_argument(
        "--analysis",
        dest="analysis",
        choices=[
            "all",
            "bitrate",
            "fluctuation",
            "rate_rebuffering",
            "buffering_ratio",
        ],
        required=True,
        help="which analysis to run",
    )
    args = event_parser.parse_args()

    # TODO: cache events to pickle files in /tmp and move analysis to a separate file
    event_df = load_data(num_of_files=args.num_files, event_num=args.event_number)

    if args.analysis == "bitrate" or "all":
        analyze_bitrate(event_df)
    elif args.analysis == "fluctuation" or "all":
        analyze_fluctuation(event_df)
    elif args.analysis == "rate_rebuffering" or "all":
        analyze_rate_rebuffering(event_df)
    elif args.analysis == "buffering_ratio" or "all":
        analyze_buffering_ratio(event_df)
