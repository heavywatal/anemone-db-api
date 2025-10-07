import datetime

import polars as pl
from fastapi import FastAPI

from . import ranemone

app = FastAPI()


@app.get("/")
def read_root() -> dict[str, str]:
    return {"Hello": "World"}


@app.get("/samples/")
def filter_samples(temp: float = -273.2, salinity: float = 0) -> list[str]:
    lf = ranemone.sample_lf().filter(
        pl.col("temp") > temp, pl.col("salinity") > salinity
    )
    column = lf.select("samplename").collect().to_series()
    return column.to_list()


@app.get("/samples/{samplename}")
def get_sample(samplename: str) -> list[dict[str, str | float | datetime.datetime]]:
    lf = ranemone.sample_lf().filter(pl.col("samplename") == samplename)
    return lf.collect().to_dicts()


@app.get("/projects/{project}")
def get_project(project: str) -> list[dict[str, str]]:
    lf = ranemone.sample_lf().filter(pl.col("project") == project)
    return lf.collect().to_dicts()


@app.get("/experiments/{samplename}")
def get_experiment(samplename: str) -> list[dict[str, str | float]]:
    lf = ranemone.experiment_lf().filter(pl.col("samplename") == samplename)
    return lf.collect().to_dicts()


@app.get("/ncopiesperml/{level}/{samplename}")
def get_community(level: str, samplename: str) -> list[dict[str, str | float | None]]:
    lf = (
        ranemone.community_lf()
        .filter(pl.col("samplename") == samplename)
        .group_by(level)
        .agg(pl.col("ncopiesperml").sum().alias("ncopiesperml"))
    )
    return lf.collect().to_dicts()
