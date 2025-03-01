# Development

The design decisions and principles.
I'm just collecting ideas now to organize them later.

## Miscelaneous ideas to be organized

Currently, the data pipeline requires manual data download and an expected structure of directories and contents. A better approach would be to:
- Systemic data download. We know what data we need and where to find it. Some datasets are automatically downloaded, but not all the requirements;
- Verification of the data. Confirm that we are using the expected data. A simple approach would be some hash of the downloaded datasets. Most of the data required is available as CSV or excel spreadsheets, so it is trivial to calculate the hash. We should be able to guarantee reproducibility and protection against corrupted data files;
- Keep a track of the local datasets. Although it is nice to have an expected data directory, that creates an overhead on building and tracking that. The raw data should be always precisely the same (for each release version). So keep a centralized dataset per machine.
- Using App's cache location is probably a good choice here.
- Simplify the library and isolate I/O (where what to load) from the analysis and transformation procedures.
- Some procedures require remote access on-demand. That makes the full pipeline vulnerable. If the server is down, the dataset can't be recreated. Instead, download all required data into a local cache. All the data should be obtained in the first run, and after that it would possible to recreate the output dataset at any moment.
- It's probably a good idea to convert some formats, such as CSV and excel, into more efficient and ready to use format such as parquet. That won't necessarily will be done in this PR.

The requirements here are:

- List of datasets required per edition of FIED;
- Source of those datasets (URLs);
- Hash of expected raw data (sha256 is a good choice here), and validation procedure;
- Be ready to minimize efforts for future releases;
