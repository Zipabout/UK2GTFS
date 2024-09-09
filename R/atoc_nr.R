#' ATOC to GTFS (Network Rail Version)
#'
#' Convert ATOC CIF files from Network Rail to GTFS
#'
#' @param paths_in Character vector, paths to Network Rail ATOC files e.g. c("C:/input/toc-full.CIF.gz", "C:/input/toc-update-mon.CIF.gz")
#' @param silent Logical, should progress messages be surpressed (default TRUE)
#' @param ncores Numeric, When parallel processing how many cores to use
#'   (default 1)
#' @param locations where to get tiploc locations (see details)
#' @param agency where to get agency.txt (see details)
#' @param shapes Logical, should shapes.txt be generated (default FALSE)
#' @family main
#' @return A gtfs list
#'
#' @details Locations
#'
#' The .msn file contains the physical locations of stations and other TIPLOC
#' codes (e.g. junctions). However, the quality of the locations is often poor
#' only accurate to about 1km and occasionally very wrong. Therefore, the
#' UK2GTFS package contains an internal dataset of the TIPLOC locations with
#' better location accuracy, which are used by default.
#'
#' However you can also specify `locations = "file"` to use the TIPLOC locations
#' in the ATOC data or provide an SF data frame of your own.
#'
#' Agency
#'
#' The ATOC files do not contain the necessary information to build the
#' agency.txt file. Therefore this data is provided with the package. You can
#' also pass your own data frame of agency information.
#'
#'
#' @export

nr2gtfs <- function(paths_in,
                    silent = TRUE,
                    ncores = 1,
                    locations = "tiplocs",
                    agency = "atoc_agency",
                    shapes = FALSE,
                    full_import = FALSE) {

  if(inherits(locations,"character")){
    if(locations == "tiplocs"){
      load_data("tiplocs")
      locations = tiplocs
    }
  }

  if(inherits(agency,"character")){
    if(agency == "atoc_agency"){
      load_data("atoc_agency")
      agency = atoc_agency
    }
  }

  # checkmate
  checkmate::assert_character(paths_in)
  checkmate::assert_logical(silent)
  checkmate::assert_numeric(ncores, lower = 1)
  checkmate::assert_logical(shapes)

  if (ncores == 1) {
    message(paste0(
      Sys.time(),
      " This will take some time, make sure you use 'ncores' to enable multi-core processing"
    ))
  }

  # Initialize empty data frames for combined results
  combined_stop_times <- data.frame()
  combined_schedule <- data.frame()

  # Process each file
  for (path_in in paths_in) {
    # Is input a zip or a folder
    if (!grepl(".gz", path_in)) {
      stop("path_in is not a .gz file")
    }

    checkmate::assert_file_exists(path_in)

    if (!silent) {
      message(paste0(Sys.time(), " Processing file: ", path_in))
    }

    # Read In each File
    mca <- importMCA(
      file = path_in,
      silent = silent,
      ncores = 1,
      full_import = full_import
    )

    if (!silent) {
      message(paste0(Sys.time(), " Processed file: ", path_in))
    }

    # Combine results
    combined_stop_times <- rbind(combined_stop_times, mca$stop_times)
    combined_schedule <- rbind(combined_schedule, mca$schedule)
  }

  # Process updates/deletes and remove duplicates
  if (!silent) {
    message(paste0(Sys.time(), " Processing Revision and Deletions in combined files"))
  }
  combined_stop_times <- process_updates(combined_stop_times)
  combined_schedule <- process_updates(combined_schedule)
  if (!silent) {
    message(paste0(Sys.time(), " Processed Revision and Deletions in combined files"))
  }
  
  # Get the Station Locations
  if ("sf" %in% class(locations)) {
    stops <- cbind(locations, sf::st_coordinates(locations))
    stops <- as.data.frame(stops)
    stops <- stops[, c(
      "stop_id", "stop_code", "stop_name",
      "Y", "X"
    )]
    names(stops) <- c(
      "stop_id", "stop_code", "stop_name",
      "stop_lat", "stop_lon"
    )
    # Remove 'Rail Station' from stop names
    stops$stop_name <- gsub(" Rail Station", "", stops$stop_name)

    stops$stop_lat <- round(stops$stop_lat, 5)
    stops$stop_lon <- round(stops$stop_lon, 5)
  } else {
    stops <- utils::read.csv(locations, stringsAsFactors = FALSE)
  }

  # Construct the GTFS
  stop_times <- combined_stop_times[, c(
    "Arrival Time",
    "Departure Time",
    "Location", "stop_sequence",
    "Activity", "rowID", "schedule"
  )]
  names(stop_times) <- c(
    "arrival_time", "departure_time", "stop_id",
    "stop_sequence", "Activity", "rowID", "schedule"
  )

  # remove any unused stops
  stops <- stops[stops$stop_id %in% stop_times$stop_id, ]

  # Main Timetable Build
  timetables <- schedule2routes(
    stop_times = stop_times,
    stops = stops,
    schedule = combined_schedule,
    silent = silent,
    ncores = ncores
  )

  # TODO: check for stop_times that are not valid stops

  timetables$agency <- agency
  timetables$stops <- stops

  # Build Shapes
  if (shapes) {
    message("Shapes are not yet supported")
  }

  return(timetables)
}

# Helper function to process updates/deletes and remove duplicates
process_updates <- function(df) {
  # Sort by UID, start date, STP Indicator, and then by the order in the original file
  df <- df[order(df$`Train UID`, df$`Date Runs From`, df$`STP Indicator`, df$rowID), ]

  # Create a unique identifier for each schedule
  df$schedule_id <- paste(df$`Train UID`, df$`Date Runs From`, df$`STP Indicator`)

  # Initialize result dataframe
  result <- data.frame()

  # Process records
  for (id in unique(df$schedule_id)) {
    subset <- df[df$schedule_id == id,]
    current_record <- NULL

    for (i in 1:nrow(subset)) {
      record <- subset[i, ]

      if (record$`Transaction Type` == "N") {
        current_record <- record
      } else if (record$`Transaction Type` == "R") {
        if (!is.null(current_record)) {
          current_record <- record
        }
      } else if (record$`Transaction Type` == "D") {
        current_record <- NULL
      }
    }

    # After processing all transactions for this schedule_id,
    # add the final state to the result (if it exists)
    if (!is.null(current_record)) {
      result <- rbind(result, current_record)
    }
  }

  # Remove temporary schedule_id column and Transaction Type
  result$schedule_id <- NULL

  return(result)
}