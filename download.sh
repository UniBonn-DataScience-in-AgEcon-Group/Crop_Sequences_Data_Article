#!/bin/bash

# Variables
URL=${URL:-"https://www.opengeodata.nrw.de/produkte/umwelt_klima/bodennutzung/landwirtschaft/LWK-TSCHLAG-HIST_EPSG25832_Shape.zip"}
URL_CUR=${URL_CUR:-"https://www.opengeodata.nrw.de/produkte/umwelt_klima/bodennutzung/landwirtschaft/LWK-TSCHLAG_EPSG25832_Shape.zip"}
OUTPUT_DIR=${OUTPUT_DIR:-"hist"}
PROJ=${PROJ:-""} # If empty, no reprojection is applied

# Ensure the output directory exists
mkdir -p "$OUTPUT_DIR"

# Define the data source using virtual file systems
DATASOURCE="/vsizip//vsicurl/${URL}"

# Extract the layer name dynamically
LAYER_NAME=$(ogrinfo -ro -al -so -nomd -q -json "$DATASOURCE" | jq -r '.layers[0].name' )

echo "Using layer: $LAYER_NAME"

# Get unique years without downloading the dataset
YEARS=$(ogrinfo -ro -q -sql "SELECT DISTINCT WJ FROM \"$LAYER_NAME\"" "$DATASOURCE" \
        | grep "WJ (Integer)" | sed 's/.*= //' | sort | uniq)

echo "Found years: $YEARS"

# Process each year
for YEAR in $YEARS; do
    echo "Processing year: $YEAR"
    
    set -f # Disable globbing
    # Construct the ogr2ogr command
    COMMAND="ogr2ogr -f Parquet \"$OUTPUT_DIR/${YEAR}.parquet\" \"$DATASOURCE\" \
              -sql \"SELECT * FROM \"$LAYER_NAME\" WHERE WJ='$YEAR'\" -lco GEOMETRY_NAME=geometry"

    # Include reprojection if specified
    if [ -n "$PROJ" ]; then
        COMMAND="$COMMAND -t_srs \"$PROJ\""
    fi

    # echo the command
    echo $COMMAND
    # Execute the command
    eval $COMMAND
done

# Download current year data
DATASOURCE_CUR="/vsizip//vsicurl/${URL_CUR}"

# Get current year layer name
LAYER_NAME_CUR=$(ogrinfo -ro -al -so -nomd -q -json "$DATASOURCE_CUR" | jq -r '.layers[0].name' )

# Get current year
YEAR=$(ogrinfo -ro -q -sql "SELECT DISTINCT WJ FROM \"$LAYER_NAME_CUR\"" "$DATASOURCE_CUR" \
        | grep "WJ (Integer)" | sed 's/.*= //' | sort | uniq)

echo "Using current year: $YEAR"

set -f # Disable globbing
COMMAND_CUR="ogr2ogr -f Parquet \"$YEAR.parquet\" \"$DATASOURCE_CUR\" \
              -sql \"SELECT ID, AREA_HA, CODE FROM \"$LAYER_NAME_CUR\"\""

if [ -n "$PROJ" ]; then
    COMMAND_CUR="$COMMAND_CUR -t_srs \"$PROJ\""
fi

echo "Processing current year"
echo $COMMAND_CUR

eval $COMMAND_CUR
