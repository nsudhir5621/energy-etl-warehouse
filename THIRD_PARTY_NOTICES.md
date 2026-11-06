# Third-party notices and licensing scope

The MIT license applies to original project software and documentation text. It
does not replace upstream rights in source data, third-party software, product
interfaces, names or logos depicted in documentation. No third-party dependency
source or binaries are vendored in this snapshot.

## EIA-derived fixtures

Source: U.S. Energy Information Administration, Form EIA-923 and EIA API v2
Electric Power Operational Data. The captured-response fixture records a capture
date of 30 September 2026; this is not an independently verified publication date.
The constructed test fixtures contain synthetic scenarios and some EIA-derived
sample numeric values. They are not a complete national observational dataset.

EIA's [copyright and reuse policy](https://www.eia.gov/about/copyrights_reuse.php)
permits reuse of its data with source acknowledgment, subject to exceptions for
protected third-party material and logos. EIA data is attributed here rather than
claimed as original MIT-licensed project content. No EIA endorsement is implied.

## Metabase reporting assets

This snapshot includes six fixture SQL questions, five bounded real-data SQL
questions and two descriptive dashboard blueprints. Metabase interface screenshots
are omitted; the genuine captures are
retained with private verification evidence. Metabase software, interface elements,
name and logos retain their upstream rights and are not relicensed under this
project's MIT grant.

See the [versioned Metabase license](https://github.com/metabase/metabase/blob/v0.50.27/LICENSE.txt)
and [upstream AGPL text](https://www.metabase.com/license/agpl). This project does
not distribute a Metabase binary or source copy. No Metabase endorsement is implied.

## Dependencies

Python packages and external PostgreSQL, dbt, Apache Airflow, Metabase and nginx
installations retain their respective licenses. Installing them separately does
not transfer their ownership to this project. The provenance review found no
conflicting vendored source, but cannot independently establish ownership of every
historical contribution. The MIT grant covers rights held by project contributors
and does not relicense third-party material.
