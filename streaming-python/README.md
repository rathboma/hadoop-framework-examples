# Streaming Python Map reduce Examples

This uses the NFL play by play dataset available at https://github.com/eljefe6a/nfldata

## Simple Example

 The simple example asks *how many stadiums have artificial turf vs natural turf*.

 From the nfl dataset, we are going to use `stadiums.csv`. However, this file was created on Windows (boo), so it has carriage returns instead of newlines.

So to prepare the file for use in unix we need to convert it:

```bash
cat ~/workspace/nfldata/stadiums.csv # notice how it's one big line

# so we convert it to newlines
dos2unix -l -n ~/workspace/nfldata/stadiums.csv ~/workspace/nfldata/unixstadiums.csv

# now it looks good
cat ~/workspace/nfldata/unixstadiums.csv #hooray! One record per line
```

Because our pipeline is very simple, and doesn't require us to join data, or do anything complex, we can test it without even using hadoop:

```bash
cat ~/workspace/nfldata/unixstadiums.csv | ./mapper.py | sort | ./reducer.py
```