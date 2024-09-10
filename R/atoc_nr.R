#' ATOC to GTFS (Network Rail Version)
#'
#' Convert ATOC CIF files from Network Rail to GTFS
#'
#' @param full_path Character, path to the full Network Rail ATOC file
#' @param update_paths Character vector, paths to Network Rail ATOC update files
#' @param silent Logical, should progress messages be suppressed (default TRUE)
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

nr2gtfs <- function(full_path,
                    update_paths = NULL,
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
  checkmate::assert_character(full_path)
  checkmate::assert_character(update_paths, null.ok = TRUE)
  checkmate::assert_logical(silent)
  checkmate::assert_numeric(ncores, lower = 1)
  checkmate::assert_logical(shapes)

  if (ncores == 1) {
    message(paste0(
      Sys.time(),
      " This will take some time, make sure you use 'ncores' to enable multi-core processing"
    ))
  }

  # Process the full CIF file
  if (!silent) {
    message(paste0(Sys.time(), " Processing full CIF file: ", full_path))
  }

  mca <- importMCA(
    file = full_path,
    silent = silent,
    ncores = 1,
    full_import = full_import,
    start_rowID = 0
  )

  combined_schedule <- mca$schedule
  combined_stop_times <- mca$stop_times

  # Process update files if provided
  if (!is.null(update_paths) && length(update_paths) > 0) {
    if (!silent) {
      message(paste0(Sys.time(), " Processing CIF file updates"))
    }
    processed_data <- process_updates_incremental(combined_schedule, combined_stop_times, update_paths, silent)
    combined_schedule <- processed_data$schedule
    combined_stop_times <- processed_data$stop_times
    if (!silent) {
      message(paste0(Sys.time(), " Processed CIF file updates"))
    }
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

process_updates_incremental <- function(schedule_df, stop_times_df, update_paths, silent = TRUE) {
  # Add schedule_id to the main dataframe once
  schedule_df$schedule_id <- paste(schedule_df$`Train UID`, schedule_df$`Date Runs From`, schedule_df$`STP indicator`, sep = "_")

  for (update_path in update_paths) {
    if (!silent) {
      message(paste0(Sys.time(), " Processing update file: ", update_path))
    }

    # Import the update file
    update_data <- importMCA(
      file = update_path,
      silent = silent,
      ncores = 1,
      full_import = FALSE,
      start_rowID = max(schedule_df$rowID)
    )

    update_schedule <- update_data$schedule
    update_stop_times <- update_data$stop_times

    # Add schedule_id to update data
    update_schedule$schedule_id <- paste(update_schedule$`Train UID`, update_schedule$`Date Runs From`, update_schedule$`STP indicator`, sep = "_")

    # Process each schedule in the update file
    for (i in 1:nrow(update_schedule)) {
      current_schedule <- update_schedule[i, ]

      if (current_schedule$`Transaction Type` == "N") {
        # New schedule: add to existing data
        if (!silent) {
          message(paste0(Sys.time(), " Processing New Schedule: ", current_schedule$schedule_id))
        }
        # Add schedule
        schedule_df <- rbind(schedule_df, current_schedule)
        stop_times_df <- rbind(
          stop_times_df,
          update_stop_times[update_stop_times$schedule == current_schedule$rowID, ]
        )
      } else if (current_schedule$`Transaction Type` == "R") {
        # Revised schedule: remove existing and add new
        if (!silent) {
          message(paste0(Sys.time(), " Processing Revise Schedule: ", current_schedule$schedule_id))
        }
        # Delete schedule
        schedule_df <- schedule_df[schedule_df$schedule_id != current_schedule$schedule_id, ]
        stop_times_df <- stop_times_df[!(stop_times_df$schedule %in% schedule_df$rowID[schedule_df$schedule_id == current_schedule$schedule_id]), ]
        # Add schedule
        schedule_df <- rbind(schedule_df, current_schedule)
        stop_times_df <- rbind(
          stop_times_df,
          update_stop_times[update_stop_times$schedule == current_schedule$rowID, ]
        )
      } else if (current_schedule$`Transaction Type` == "D") {
        # Deleted schedule: remove existing
        if (!silent) {
          message(paste0(Sys.time(), " Processing Delete Schedule: ", current_schedule$schedule_id))
        }
        schedule_df <- schedule_df[schedule_df$schedule_id != current_schedule$schedule_id, ]
        stop_times_df <- stop_times_df[!(stop_times_df$schedule %in% schedule_df$rowID[schedule_df$schedule_id == current_schedule$schedule_id]), ]
      }
    }

    if (!silent) {
      message(paste0(Sys.time(), " Processed update file: ", update_path))
    }

  }

  # Remove temporary schedule_id column
  schedule_df$schedule_id <- NULL

  return(list(schedule = schedule_df, stop_times = stop_times_df))
}