#!/bin/bash

# Script to transform MIN aggregate queries to SELECT DISTINCT queries
# Usage: ./transform_job_distinct.sh input_directory output_directory

if [ $# -ne 2 ]; then
    echo "Usage: $0 input_directory output_directory"
    exit 1
fi

INPUT_DIR="$1"
OUTPUT_DIR="$2"

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Input directory $INPUT_DIR not found"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Function to extract MIN columns from SQL file
extract_min_columns() {
    local file="$1"
    
    # Use awk to properly parse MIN expressions across multiple lines
    awk '
    BEGIN { in_select = 0; select_text = "" }
    
    /^SELECT/ { in_select = 1; select_text = $0; next }
    
    in_select && /^FROM/ { in_select = 0; print select_text; next }
    
    in_select { select_text = select_text " " $0 }
    
    END { if (in_select) print select_text }
    ' "$file" | grep -oP 'MIN\(\s*\K[^)]+(?=\s*\))' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

# Function to transform a single query file to SELECT DISTINCT
transform_query_distinct() {
    local input_file="$1"
    local output_file="$2"
    
    echo "Transforming: $(basename "$input_file")"
    
    # Extract MIN columns
    local min_cols=($(extract_min_columns "$input_file"))
    
    if [ ${#min_cols[@]} -eq 0 ]; then
        echo "  No MIN aggregates found, copying original file"
        cp "$input_file" "$output_file"
        return
    fi
    
    echo "  Found MIN columns: ${min_cols[*]}"
    
    # Build new SELECT DISTINCT clause
    local new_select="SELECT DISTINCT "
    
    for i in "${!min_cols[@]}"; do
        local col="${min_cols[$i]}"
        
        if [ $i -gt 0 ]; then
            new_select="$new_select,\n                 "
        fi
        
        new_select="$new_select$col"
    done
    
    # Create the transformed query
    {
        echo -e "$new_select"
        
        # Copy FROM clause onwards
        sed -n '/^FROM/,$ p' "$input_file"
        
    } > "$output_file"
    
    echo "  Transformation completed"
}

# Process all SQL files
total_files=0
transformed_files=0

find "$INPUT_DIR" -name "*.sql" -type f | sort | while read -r sql_file; do
    total_files=$((total_files + 1))
    
    # Get relative path from input directory
    relative_path=$(realpath --relative-to="$INPUT_DIR" "$sql_file")
    
    # Create corresponding output file path
    output_file="$OUTPUT_DIR/$relative_path"
    
    # Create output subdirectories if needed
    output_subdir=$(dirname "$output_file")
    mkdir -p "$output_subdir"
    
    # Transform the query
    transform_query_distinct "$sql_file" "$output_file"
    
    # Check if transformation actually changed the file
    if ! diff -q "$sql_file" "$output_file" > /dev/null 2>&1; then
        transformed_files=$((transformed_files + 1))
    fi
done

echo ""
echo "============================================="
echo "Transformation Summary:"
echo "  Input directory: $INPUT_DIR"
echo "  Output directory: $OUTPUT_DIR"
echo "  Files processed: $total_files"
echo "  Files transformed: $transformed_files"
echo "============================================="
echo ""
echo "Transformed files:"
find "$OUTPUT_DIR" -name "*.sql" -type f | sort | while read -r file; do
    echo "  $(realpath --relative-to="$OUTPUT_DIR" "$file")"
done