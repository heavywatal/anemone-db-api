from pathlib import Path

import polars as pl

cache_dir = Path("~/.cache/ranemone").expanduser()


def main() -> None:
    x = sample_lf()
    print(x.head().collect())
    y = experiment_lf()
    print(y.head().collect())
    z = community_lf()
    print(z.head().collect())


def sample_lf() -> pl.LazyFrame:
    return pl.scan_csv(
        cache_dir / "sample.tsv.gz",
        separator="\t",
        schema_overrides={"samp_size": pl.Float64},
        try_parse_dates=True,
    )


def experiment_lf() -> pl.LazyFrame:
    return pl.scan_csv(
        cache_dir / "experiment.tsv.gz",
        separator="\t",
        schema_overrides={"samp_vol_we_dna_ext": pl.Float64},
    )


def community_lf() -> pl.LazyFrame:
    return pl.scan_csv(
        cache_dir / "community_qc3nn_target.tsv.gz",
        separator="\t",
        schema_overrides={"ncopiesperml": pl.Float64},
    )


if __name__ == "__main__":
    main()
