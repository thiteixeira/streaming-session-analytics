#!.venv/bin/python

import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

pd.set_option("display.max_rows", None)
sns.set_theme(style="whitegrid")

EVENT_1 = 38
EVENT_2 = 12
EVENT_3 = 1
EVENT_4 = 4
MIN_SESSION_DURATION = 6
PLAYER_PING_MIN = 10 / 60

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
    """Compute the number of rendition changes per minute"""

    print("Analyzing fluctuation ...")
    df = (
        event_df[["session", "seq", "rendition_width"]]
        .groupby("session")
        # Filter on session duration >= 6 (one minute)
        .filter(lambda x: x["seq"].count() >= MIN_SESSION_DURATION)
        .reset_index(drop=True)
    )

    # Compute the fluctuation rate per session id
    df["num_of_changes"] = df.groupby("session").rendition_width.apply(
        lambda x: x.rolling(2).apply(lambda x: x.iloc[0] < x.iloc[1])
    )

    out = (
        df[["session", "num_of_changes"]]
        .groupby(["session"])
        .apply(lambda x: x["num_of_changes"].sum() / (len(x) * PLAYER_PING_MIN))
    )

    # Plot the CDF of the fluctuation rate
    ax = sns.ecdfplot(data=out, legend=None)
    ax.set_title("CDF of Fluctuation Rate")
    ax.set(xlabel="Rate of fluctuations (per minute)", ylabel="CDF")
    plt.savefig("./assets/cdf_fluctuation.png")
    plt.show()


def analyze_rate_rebuffering(event_df: pd.DataFrame) -> None:
    """Compute the number of buffering events per minute over the session duration"""

    print("Analyzing fluctuation ...")
    df = (
        event_df[["session", "seq", "rebuffering_count"]]
        .groupby("session")
        # Filter on session duration >= 6 (one minute)
        .filter(lambda x: x["seq"].count() >= MIN_SESSION_DURATION)
        .reset_index(drop=True)
    )

    # Compute the rebuffering rate per session id
    df["num_of_changes"] = df.groupby("session").rebuffering_count.apply(
        lambda x: x.rolling(2).apply(lambda x: x.iloc[0] < x.iloc[1])
    )

    out = (
        df[["session", "num_of_changes"]]
        .groupby(["session"])
        .apply(lambda x: x["num_of_changes"].sum() / (len(x) * PLAYER_PING_MIN))
    )

    # Plot the CDF of the fluctuation rate
    ax = sns.ecdfplot(data=out, legend=None)
    ax.set_title("CDF of Rebuffering Rate")
    ax.set(xlabel="Rate of buffering events (per minute)", ylabel="CDF")
    plt.savefig("./assets/cdf_rebuffering.png")
    plt.show()


def analyze_buffering_ratio(event_df: pd.DataFrame) -> None:
    """Compute the fraction of time spent in buffering events over the duration of the video session"""

    print("Analyzing buffering ratio ...")
    df = (
        event_df[["session", "seq", "rebuffering_seconds", "rebuffering_count"]]
        .groupby("session")
        # Filter on session duration >= 6 (one minute)
        .filter(lambda x: x["seq"].count() >= MIN_SESSION_DURATION)
        .reset_index(drop=True)
    )
    print(df.head(n=200))


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

    if args.analysis in ["bitrate", "all"]:
        analyze_bitrate(event_df)
    elif args.analysis in ["fluctuation", "all"]:
        analyze_fluctuation(event_df)
    elif args.analysis in ["rate_rebuffering", "all"]:
        analyze_rate_rebuffering(event_df)
    elif args.analysis in ["buffering_ratio", "all"]:
        analyze_buffering_ratio(event_df)
