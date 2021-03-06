# Tutorial 7: High Performance Data Analytics with R (package: bigmemory) 

# ============================================================
# How to manage the job execution.

# Execute this code like so:
# $ qsub pbs_R_bigmemory_7.sh

# To monitor progress
# $ qstat -u your_user_name

# To view progressive output
# qpeek 466569

# To terminate the job prematurely:
# $ qsig -s SIGINT 466569
# OR
# qdel 466569

# ============================================================
# What the code does

# This example queries the humungous big.matrix file.
# It does use multi-core computing on one node.
# It does demonstrated how to benchmark R code for performance.

# This example shows:
# 1. how to refactor code into subroutines
# 2. how to calculate time differences in seconds.
# 4. How to display progress information.
# 6. How to manage very large data sets in the R environment.

# bigmemory provides only core functionality and depends on 
# the packages synchronicity, biganalytics, bigalgebra, bigtabulate
# to actually do stuff.

# ============================================================
# Data preparation

# The data needs to have the tailnum values mapped to integers.
# In the data file plane-data.csv, tailnum's look like: N102UW.
# The c++ code in map_string_fields.cpp performs 
# the necessary preprocessing on the (1987...2008).csv raw data files.
# The preprocessed csv files are stored in the preprocessed folder
# and then converted to big.matrix files and stored in the big_matrices folder.
# Lots of flights do not have a tailnum for the plane. These are excluded.

# ============================================================
# Setup

# Within the R environment verify that the bigmemory packages is installed.
# > installed.packages('bigmemory')
# If it is not, install it like so:
# > install.packages('bigmemory')

library(bigmemory)
library(foreach)
library(doMC)

# ============================================================
# Constants.

project_storage_path <- "/lustre/pVPAC0012"
# project_storage_path <- "/Users/developer/git/R_data_analytics_bigmemory_foreach_tutorial"
input_folder_path <- paste(project_storage_path, "big_matrices", sep = "/")
output_folder_path <- paste(project_storage_path, "big_matrices", sep = "/")

# The full job
descriptor_name <- "all.desc"
# A short job for testing.
# descriptor_name <- "2008.desc"

flight_field_names <- list (
    "Year",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "DepTime",
    "CRSDepTime",
    "ArrTime",
    "CRSArrTime",
    "UniqueCarrier",
    "FlightNum",
    "TailNum",
    "ActualElapsedTime",
    "CRSElapsedTime",
    "AirTime",
    "ArrDelay",
    "DepDelay",
    "Origin",
    "Dest",
    "Distance",
    "TaxiIn",
    "TaxiOut",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay")

# Defined in map_string_fields.cpp
missing_value <- -1

# ============================================================
# Function definitions.

attach_bigmatrix = function (file_name_desc) {
    file_path_desc <- paste(input_folder_path, file_name_desc, sep = "/")
    cat("\nFile: ", file_name_desc)
    datadesc <- dget(file_path_desc)
    matrix_0 = attach.big.matrix(datadesc, path=input_folder_path)
    return(matrix_0)
}

# ============================================================
# Parallel processing setup.

# Tell node to use 8 cores.
registerDoMC(8)

# Just to reassure ourselves.
print (getDoParWorkers())
getDoParName()

# ============================================================
# Process the data.

# Attach the whole result matrix.   

# Benchmark start time.
start_time <- Sys.time()

all_flights <- attach_bigmatrix(descriptor_name)

# Benchmark stop time and record duration.
duration = difftime(Sys.time(), start_time, units = "secs")
cat("\nAttach all flights matrix duration/sec: ", duration, "\n")

# Benchmark start time.
start_time <- Sys.time()

# Query the whole result matrix.
# TailNum is at column (plane_id_index) 11

plane_id_index = which("TailNum" == flight_field_names)[[1]]
year_index = which("Year" == flight_field_names)[[1]]
month_index = which("Month" == flight_field_names)[[1]]

planes = unique(all_flights[, plane_id_index])
planes = setdiff(planes, missing_value) # The "missing value" item is discarded
plane_count <- length(planes)

# Benchmark stop time and record duration.
duration = difftime(Sys.time(), start_time, units = "secs")
cat("\nPlane count duration/sec: ", duration, "\n")

# Benchmark start time.
start_time <- Sys.time()

# Distributed query for the earliest date each plane started flying.
# For each plane all of its flights are found by mwhich.
# Then just the dates (year, month) for the plane's flights are returned.
# Then the earliest date is returned 
# and then aggregrated with the others.

plane_start <- foreach(plane_index = 1:plane_count, .combine=rbind, .packages=c('doMC', 'bigmemory')) %dopar% {

    cat("\nPlane: ", plane_index, ": ", planes[plane_index])
    
    # Not needed with doMC but it is with multi-node processing.
    # all_flights <- attach_bigmatrix(descriptor_name)
    
    # Flight dates for one plane.
    # All on one line to ensure isolation as different processes access the all_flights matrix.
    flight_dates <- all_flights[mwhich(all_flights, plane_id_index, planes[plane_index], 'eq'), 
                        c(year_index, month_index), drop=FALSE]
    year_idx = 1
    month_idx = 2           
    min_year <- min(flight_dates[, year_idx], na.rm=TRUE)
    dates_in_first_year_indices <- which(flight_dates[, year_idx] == min_year)
    min_month_in_min_year <- min(flight_dates[dates_in_first_year_indices, month_idx], na.rm=TRUE)
	record <- c(planes[plane_index], min_year, min_month_in_min_year)
}

# Output looks like:
#             [,1] [,2] [,3]
# result.1    3417 2008    1
# result.2    3743 2008    1
# ......................
# result.4880 3758 2008    4

# Report results
for (plane_index in 1:nrow(plane_start)) {
    cat("\nFirst flight for: ", plane_start[plane_index, 1], "on: ", plane_start[plane_index, 2], "/", plane_start[plane_index, 3]) 
}
cat("\n")

# Benchmark stop time and record duration.
duration = difftime(Sys.time(), start_time, units = "secs")
cat("\nQuery duration/sec: ", duration, "\n")
   
# ============================================================

# Clear out the workspace.
#rm(list = ls())