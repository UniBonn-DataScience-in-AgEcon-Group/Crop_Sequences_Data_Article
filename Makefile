URL ?= "https://www.opengeodata.nrw.de/produkte/umwelt_klima/bodennutzung/landwirtschaft/LWK-TSCHLAG-HIST_EPSG25832_Shape.zip"
URL_CUR ?= "https://www.opengeodata.nrw.de/produkte/umwelt_klima/bodennutzung/landwirtschaft/LWK-TSCHLAG_EPSG25832_Shape.zip"
OUTPUT_DIR ?= "hist"
PROJ ?= "WGS84"

START_YEAR ?= 2019
END_YEAR ?= 2024

SHELL = /bin/bash

.PHONY: download join clean

download: $(END_YEAR).parquet

join: Crop_Sequences_NRW_$(START_YEAR)_$(END_YEAR).parquet

clean:
	rm -f "$(END_YEAR).parquet"
	rm -rf "$(OUTPUT_DIR)"

$(END_YEAR).parquet:
	[ -f "$@" ] || ( \
		echo "Downloading data using script..."; \
		sh ./download.sh ;\
    )

Crop_Sequences_NRW_$(START_YEAR)_$(END_YEAR).parquet: $(END_YEAR).parquet
	[ -f "$@" ] || ( \
		echo "Joining data..."; \
		uv run join-plots.py --cur "./$(END_YEAR).parquet" --hist "./$(OUTPUT_DIR)" --out "./$@" \
	)

delete:
	rm -f "$(END_YEAR).parquet"
	rm -rf "$(OUTPUT_DIR)"