# postgres-dupe-fixer

This script was designed to fix duplicates in a [Lemmy](https://github.com/LemmyNet/lemmy) database that exist due to a corrupted unique index not enforcing its constraints.

It should be easy to adjust for other tables if needed.

Foreign key references are automatically detected to update all references pointing to the duplicate rows.

To minimize the risk of data loss, all related changes are grouped in transactions and row counts in other tables are verified to ensure no rows are dropped from a cascaded deletion when the duplicate row is deleted.

Unfortunately, this turned out to not be as simple as originally anticipated, as certain tables in Lemmy ended up having conflicts following this, which would have had to be resolved separately.

This repo is now archived in case it might be useful as a reference for something else in the future.
