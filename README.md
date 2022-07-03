# Streaming Dataset Analysis

Further analysis of the Streaming Dataset paper

## Running the code

Create a virtual environment:

```bash
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

Run with arguments, where the arguments are the following:

- Event Number (--evt-num): The event number to analyze [1, 2, 3, 4]. Default: 1
- Number of files to pull (--num-files): The number of files to load from GitHub. If no number is given, all files will be loaded
- Type of analysis (--analysis): **Required.** Which analysis to run. Options are:
  - "all": Run all analyses
  - "bitrate": Run the bitrate analysis
  - "fluctuation": Run the fluctuation analysis
  - "rate_rebuffering": Run the rate rebuffering analysis
  - "buffering_ratio": Run the buffering ratio analysis

```bash
# Run the code with default arguments
$ ./main.py --analysis=all

# Run the code with custom arguments
$ ./main.py --evt-num=1 --num-files=1 --analysis=bitrate

# Help
$ ./main.py --help
```

## Event Stats

### Average Bitrate

Calculated as the average bitrate of the requested renditions over time.

### Rate of Fluctuations

How often (per minute) the rendition changes

### Rate of Buffering

The number of buffering events per minute over the session duration

### Buffering Ratio

The fraction of time spent in buffering events over the duration of the video session
